#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "memory.h"
#include "table.h"
#include "blockio.h"
#include "join.h"

/* ==== 조인 단계별 시간 측정용 전역 변수 ==== */
double g_time_read  = 0.0;
double g_time_join  = 0.0;
double g_time_write = 0.0;

static double diff_sec(const struct timespec *start, const struct timespec *end)
{
    return (end->tv_sec  - start->tv_sec)
         + (end->tv_nsec - start->tv_nsec) / 1e9;
}

static uint64_t hash_string(const char *s)
{
    uint64_t h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h;
}

static size_t next_pow2(size_t x)
{
    if (x <= 1) return 1;
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static void* arena_calloc_local(Arena *a, size_t n, size_t sz)
{
    size_t bytes = n * sz;
    void *p = arena_alloc(a, bytes);
    memset(p, 0, bytes);
    return p;
}

typedef struct HashNode {
    uint64_t h;
    char *key;
    char *nonkey;
    struct HashNode *next;
} HashNode;

static unsigned int clamp_u32(unsigned int v, unsigned int lo, unsigned int hi)
{
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

/* I/O 버퍼: 메모리 제한에 포함되도록 big_alloc로 잡아서 setvbuf에 전달 */
typedef struct {
    size_t out_buf_sz;
    size_t part_buf_sz;
    size_t total_bytes; /* out + part(2P) */
} IoBufPlan;

static IoBufPlan plan_iobuf(size_t max_mem, unsigned int P)
{
    IoBufPlan plan = {0};

    if (max_mem == 0) {
        plan.out_buf_sz  = 1 << 20;  /* 1MB */
        plan.part_buf_sz = 16 << 10; /* 16KB */
        plan.total_bytes = plan.out_buf_sz + plan.part_buf_sz * (size_t)(2 * P);
        return plan;
    }

    /* 전체 예산: max_mem의 1/16(6.25%), 최소 64KB, 최대 8MB */
    size_t io_budget = max_mem / 16;
    if (io_budget < (64 * 1024)) io_budget = 64 * 1024;
    if (io_budget > (8ULL << 20)) io_budget = 8ULL << 20;

    /* output에 절반까지(최대 1MB, 최소 64KB) */
    size_t out_buf = io_budget / 2;
    if (out_buf < (64 * 1024)) out_buf = 64 * 1024;
    if (out_buf > (1 << 20))   out_buf = 1 << 20;

    size_t remain = (io_budget > out_buf) ? (io_budget - out_buf) : 0;

    /* 파티션 파일(2P개)에 분배: 최소 256B, 최대 64KB */
    size_t part_buf = 0;
    if (P > 0) {
        part_buf = remain / (size_t)(2 * P);
        if (part_buf < 256) part_buf = 256;
        if (part_buf > (64 * 1024)) part_buf = 64 * 1024;
    }

    plan.out_buf_sz  = out_buf;
    plan.part_buf_sz = part_buf;
    plan.total_bytes = out_buf + part_buf * (size_t)(2 * P);
    return plan;
}

void left_join_strategyB(const Table *left,
                         const Table *right,
                         const char *output_path,
                         int *left_block_count_out,
                         int *right_block_count_out)
{
    size_t B_L = left->block_size;
    size_t B_R = right->block_size;

    struct timespec tj_start, tj_end;
    clock_gettime(CLOCK_MONOTONIC, &tj_start);

    /* ---- 기본 유효성 검사 ---- */
    if (B_L < sizeof(BlockHeader) + sizeof(uint16_t) + MIN_REC_BYTES ||
        B_R < sizeof(BlockHeader) + sizeof(uint16_t) + MIN_REC_BYTES) {
        fprintf(stderr, "[ERROR] block_size too small: B_L=%zu, B_R=%zu\n", B_L, B_R);
        exit(EXIT_FAILURE);
    }

    int max_left_recs  = (int)(B_L / (sizeof(uint16_t) + MIN_REC_BYTES));
    int max_right_recs = (int)(B_R / (sizeof(uint16_t) + MIN_REC_BYTES));
    if (max_left_recs <= 0 || max_right_recs <= 0) {
        fprintf(stderr, "[ERROR] block_size too small for records: B_L=%zu, B_R=%zu\n", B_L, B_R);
        exit(EXIT_FAILURE);
    }

    /* ---- 원본 파일 크기 파악 ---- */
    struct stat stL, stR;
    size_t left_file_size  = 0;
    size_t right_file_size = 0;
    if (stat(left->filename, &stL) == 0)  left_file_size  = (size_t)stL.st_size;
    if (stat(right->filename, &stR) == 0) right_file_size = (size_t)stR.st_size;

    /* ---- 고정 버퍼(big_alloc) ---- */
    Block *blkL = (Block*)big_alloc(B_L);
    Block *blkR = (Block*)big_alloc(B_R);
    char **recsL = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);
    char **recsR = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

    size_t safety = 64 * 1024;

    /* ---- P / I/O buffer / arena_cap를 고정점으로 맞추기(최대 5회) ---- */
    unsigned int P = 1, prevP = 0;
    IoBufPlan io = {0}, prevIo = {0};

    for (int it = 0; it < 5; it++) {
        size_t used_now = g_big_alloc_bytes;
        size_t io_bytes = (prevIo.total_bytes ? prevIo.total_bytes : 0);

        if (g_max_memory_bytes > 0) {
            if (used_now + safety + io_bytes >= g_max_memory_bytes) {
                fprintf(stderr,
                        "[ERROR] max_mem too small: used=%zu safety=%zu io=%zu limit=%zu\n",
                        used_now, safety, io_bytes, g_max_memory_bytes);
                exit(EXIT_FAILURE);
            }
        }

        size_t rem = (g_max_memory_bytes > 0)
                   ? (g_max_memory_bytes - used_now - safety - io_bytes)
                   : (128ULL << 20);

        size_t target_part_disk = rem / 2;
        if (target_part_disk < 256 * 1024) target_part_disk = 256 * 1024;

        unsigned int newP = 1;
        if (right_file_size > 0) {
            newP = (unsigned int)((right_file_size + target_part_disk - 1) / target_part_disk);
            if (newP < 1) newP = 1;
            newP = clamp_u32(newP, 1u, 256u);
        }

        IoBufPlan newIo = plan_iobuf(g_max_memory_bytes, newP);

        if (newP == P && newIo.total_bytes == io.total_bytes && it > 0) break;

        prevP = P;
        prevIo = io;
        P = newP;
        io = newIo;

        if (prevP == P && prevIo.total_bytes == io.total_bytes) break;
    }

    /* ---- I/O 버퍼 big_alloc로 확보(메모리 제한에 포함) ---- */
    char *out_iobuf = NULL;
    char *part_iobuf_pool = NULL;

    if (io.out_buf_sz > 0) {
        out_iobuf = (char*)big_alloc(io.out_buf_sz);
    }
    if (io.part_buf_sz > 0 && P > 0) {
        size_t total = io.part_buf_sz * (size_t)(2 * P);
        part_iobuf_pool = (char*)big_alloc(total);
    }

    /* ---- 남는 메모리 거의 전부를 arena로 ---- */
    size_t arena_cap = 0;
    if (g_max_memory_bytes > 0) {
        if (g_big_alloc_bytes + safety >= g_max_memory_bytes) {
            fprintf(stderr,
                    "[ERROR] no room for arena: used=%zu safety=%zu limit=%zu\n",
                    g_big_alloc_bytes, safety, g_max_memory_bytes);
            exit(EXIT_FAILURE);
        }
        arena_cap = g_max_memory_bytes - g_big_alloc_bytes - safety;
        if (arena_cap < 128 * 1024) {
            fprintf(stderr, "[WARN] arena_cap very small: %zu\n", arena_cap);
        }
    } else {
        arena_cap = 128ULL << 20;
    }

    Arena arena;
    arena_init(&arena, arena_cap);

    printf("[INFO] Partitioned hash left join (partition scheme preserved)\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n", g_max_memory_bytes, B_L, B_R);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu\n", left_file_size, right_file_size);
    printf("[INFO] fixed_big_alloc=%zu, arena_cap=%zu, P=%u\n", g_big_alloc_bytes, arena_cap, P);
    printf("[INFO] iobuf: out=%zu, part_each=%zu (total=%zu)\n",
           io.out_buf_sz, io.part_buf_sz, io.total_bytes);

    /* ---- 파티션 파일 준비 ---- */
    char **left_part_paths  = (char**)malloc(sizeof(char*) * P);
    char **right_part_paths = (char**)malloc(sizeof(char*) * P);
    FILE **lfp = (FILE**)malloc(sizeof(FILE*) * P);
    FILE **rfp = (FILE**)malloc(sizeof(FILE*) * P);
    size_t *right_part_rec_counts = (size_t*)calloc(P, sizeof(size_t));

    if (!left_part_paths || !right_part_paths || !lfp || !rfp || !right_part_rec_counts) {
        perror("alloc partition structures");
        exit(EXIT_FAILURE);
    }

    for (unsigned int p = 0; p < P; p++) {
        left_part_paths[p]  = (char*)malloc(64);
        right_part_paths[p] = (char*)malloc(64);
        if (!left_part_paths[p] || !right_part_paths[p]) {
            perror("malloc partition path");
            exit(EXIT_FAILURE);
        }

        snprintf(left_part_paths[p],  64, "left_part_%u.tmp",  p);
        snprintf(right_part_paths[p], 64, "right_part_%u.tmp", p);

        lfp[p] = fopen(left_part_paths[p], "w+");
        if (!lfp[p]) { perror("fopen left partition"); exit(EXIT_FAILURE); }

        rfp[p] = fopen(right_part_paths[p], "w+");
        if (!rfp[p]) { perror("fopen right partition"); exit(EXIT_FAILURE); }

        /* big_alloc 버퍼를 stream에 지정 (메모리 제한에 포함) */
        if (part_iobuf_pool && io.part_buf_sz > 0) {
            char *bufL = part_iobuf_pool + (size_t)(2 * p) * io.part_buf_sz;
            char *bufR = part_iobuf_pool + (size_t)(2 * p + 1) * io.part_buf_sz;
            setvbuf(lfp[p], bufL, _IOFBF, io.part_buf_sz);
            setvbuf(rfp[p], bufR, _IOFBF, io.part_buf_sz);
        }
    }

    /* ---- Phase 1: 원본을 파티션 파일로 분할 ---- */
    int left_blocks = 0, right_blocks = 0;

    /* left */
    {
        FILE *lf = fopen(left->filename, "r");
        if (!lf) { perror("fopen left"); exit(EXIT_FAILURE); }

        PendingLine pend = {0};

        while (1) {
            int cnt = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(lf, B_L, blkL, recsL, &cnt, &pend, max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || cnt == 0) break;
            left_blocks++;

            for (int i = 0; i < cnt; i++) {
                char key[256];
                get_field(recsL[i], left->header.key_index, key, sizeof(key));
                unsigned int part = (unsigned int)(hash_string(key) % P);

                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fputs(recsL[i], lfp[part]);
                fputc('\n', lfp[part]);
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);
            }
        }

        pendingline_free(&pend);
        fclose(lf);
    }

    /* right */
    {
        FILE *rf = fopen(right->filename, "r");
        if (!rf) { perror("fopen right"); exit(EXIT_FAILURE); }

        PendingLine pend = {0};

        while (1) {
            int cnt = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rf, B_R, blkR, recsR, &cnt, &pend, max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || cnt == 0) break;
            right_blocks++;

            for (int i = 0; i < cnt; i++) {
                char key[256];
                get_field(recsR[i], right->header.key_index, key, sizeof(key));
                unsigned int part = (unsigned int)(hash_string(key) % P);

                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fputs(recsR[i], rfp[part]);
                fputc('\n', rfp[part]);
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);

                right_part_rec_counts[part]++;
            }
        }

        pendingline_free(&pend);
        fclose(rf);
    }

    for (unsigned int p = 0; p < P; p++) {
        fflush(lfp[p]);
        fflush(rfp[p]);
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);
    }

    /* ---- Phase 2: 파티션별 해시 left join ---- */
    FILE *out = fopen(output_path, "w");
    if (!out) { perror("fopen join output"); exit(EXIT_FAILURE); }
    if (out_iobuf && io.out_buf_sz > 0) {
        setvbuf(out, out_iobuf, _IOFBF, io.out_buf_sz);
    }

    int nonkey_cnt = right->header.num_columns - 1;

    for (unsigned int p = 0; p < P; p++) {
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);

        if (right_part_rec_counts[p] == 0) {
            PendingLine pendL = {0};
            while (1) {
                int lc = 0;
                struct timespec tr1, tr2;
                clock_gettime(CLOCK_MONOTONIC, &tr1);
                int ok = fill_block(lfp[p], B_L, blkL, recsL, &lc, &pendL, max_left_recs);
                clock_gettime(CLOCK_MONOTONIC, &tr2);
                g_time_read += diff_sec(&tr1, &tr2);

                if (!ok || lc == 0) break;

                for (int i = 0; i < lc; i++) {
                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                    fputs(recsL[i], out);
                    for (int k = 0; k < nonkey_cnt; k++) fputs("NULL|", out);
                    fputc('\n', out);
                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);
                }
            }
            pendingline_free(&pendL);
            continue;
        }

        arena_reset(&arena);

        size_t rN = right_part_rec_counts[p];
        size_t bucket_count = next_pow2(rN * 2);
        if (bucket_count < 1024) bucket_count = 1024;

        /* 버킷 배열이 arena를 너무 먹지 않도록(최대 arena_cap/4) */
        size_t max_bucket_bytes = arena_cap / 4;
        while (bucket_count * sizeof(HashNode*) > max_bucket_bytes && bucket_count > 1024) {
            bucket_count >>= 1;
        }

        HashNode **buckets = (HashNode**)arena_calloc_local(&arena, bucket_count, sizeof(HashNode*));

        /* build hash from R_p */
        PendingLine pendR = {0};
        while (1) {
            int rc = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rfp[p], B_R, blkR, recsR, &rc, &pendR, max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || rc == 0) break;

            for (int i = 0; i < rc; i++) {
                char key_buf[256];
                get_field(recsR[i], right->header.key_index, key_buf, sizeof(key_buf));

                char nonkey_buf[4096];
                build_nonkey_fields(right, recsR[i], nonkey_buf, sizeof(nonkey_buf));

                uint64_t h = hash_string(key_buf);
                size_t idx = (size_t)(h & (bucket_count - 1));

                HashNode *node = (HashNode*)arena_alloc(&arena, sizeof(HashNode));
                node->h      = h;
                node->key    = arena_strdup(&arena, key_buf);
                node->nonkey = arena_strdup(&arena, nonkey_buf);
                node->next   = buckets[idx];
                buckets[idx] = node;
            }
        }
        pendingline_free(&pendR);

        /* probe with L_p */
        PendingLine pendL = {0};
        while (1) {
            int lc = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(lfp[p], B_L, blkL, recsL, &lc, &pendL, max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || lc == 0) break;

            for (int i = 0; i < lc; i++) {
                char key_buf[256];
                get_field(recsL[i], left->header.key_index, key_buf, sizeof(key_buf));

                uint64_t h = hash_string(key_buf);
                size_t idx = (size_t)(h & (bucket_count - 1));

                int matched = 0;
                for (HashNode *n = buckets[idx]; n; n = n->next) {
                    if (n->h == h && strcmp(n->key, key_buf) == 0) {
                        matched = 1;
                        struct timespec tw1, tw2;
                        clock_gettime(CLOCK_MONOTONIC, &tw1);
                        fputs(recsL[i], out);
                        fputs(n->nonkey, out);
                        fputc('\n', out);
                        clock_gettime(CLOCK_MONOTONIC, &tw2);
                        g_time_write += diff_sec(&tw1, &tw2);
                    }
                }

                if (!matched) {
                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                    fputs(recsL[i], out);
                    for (int k = 0; k < nonkey_cnt; k++) fputs("NULL|", out);
                    fputc('\n', out);
                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);
                }
            }
        }
        pendingline_free(&pendL);
    }

    fclose(out);

    /* cleanup tmp */
    for (unsigned int p = 0; p < P; p++) {
        if (lfp[p]) fclose(lfp[p]);
        if (rfp[p]) fclose(rfp[p]);
        remove(left_part_paths[p]);
        remove(right_part_paths[p]);
        free(left_part_paths[p]);
        free(right_part_paths[p]);
    }
    free(lfp);
    free(rfp);
    free(left_part_paths);
    free(right_part_paths);
    free(right_part_rec_counts);

    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total = diff_sec(&tj_start, &tj_end);
    g_time_join = total - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks;
    *right_block_count_out = right_blocks;
}
