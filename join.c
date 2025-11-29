/* join.c : PARTITIONED HASH LEFT JOIN (strict memory + big_alloc iobuf) */
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

/* ==== timing globals ==== */
double g_time_read  = 0.0;
double g_time_join  = 0.0;
double g_time_write = 0.0;

static double diff_sec(const struct timespec *s, const struct timespec *e) {
    return (e->tv_sec - s->tv_sec) + (e->tv_nsec - s->tv_nsec) / 1e9;
}

static void reset_timers_local(void) {
    g_time_read = 0.0;
    g_time_join = 0.0;
    g_time_write = 0.0;
}

/* pendingline_free() 심볼 없을 때를 대비한 로컬 해제 */
static void pendingline_reset_local(PendingLine *p) {
    if (!p) return;
    p->pending_len = 0;
}

/* FNV-1a 64-bit */
static uint64_t hash_string(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h;
}

static size_t next_pow2(size_t x) {
    if (x <= 1) return 1;
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static unsigned int clamp_u32(unsigned int v, unsigned int lo, unsigned int hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static void* arena_calloc_local(Arena *a, size_t n, size_t sz) {
    size_t bytes = n * sz;
    void *p = arena_alloc(a, bytes);
    if (!p) return NULL;
    memset(p, 0, bytes);
    return p;
}

/* hash node stored in arena */
typedef struct HashNode {
    uint64_t h;
    char *key;
    char *nonkey;
    struct HashNode *next;
} HashNode;

/* I/O buffer plan: all buffers come from big_alloc so they count to max_mem */
typedef struct {
    size_t out_buf_sz;
    size_t inL_buf_sz;
    size_t inR_buf_sz;
    size_t part_buf_sz;
    size_t total_bytes; /* out + inL + inR + part(2P) */
} IoBufPlanP;

static IoBufPlanP plan_iobuf_partition(size_t max_mem, unsigned int P) {
    IoBufPlanP plan;
    memset(&plan, 0, sizeof(plan));

    if (max_mem == 0) {
        plan.out_buf_sz  = 1 << 20;  /* 1MB */
        plan.inL_buf_sz  = 1 << 20;  /* 1MB */
        plan.inR_buf_sz  = 1 << 20;  /* 1MB */
        plan.part_buf_sz = 16 << 10; /* 16KB */
        plan.total_bytes = plan.out_buf_sz + plan.inL_buf_sz + plan.inR_buf_sz
                         + plan.part_buf_sz * (size_t)(2 * P);
        return plan;
    }

    size_t io_budget = max_mem / 16;              /* 6.25% */
    if (io_budget < 128 * 1024) io_budget = 128 * 1024;
    if (io_budget > (8ULL << 20)) io_budget = 8ULL << 20;

    size_t out_buf = io_budget / 2;
    size_t in_each = io_budget / 8;

    if (out_buf < 64 * 1024) out_buf = 64 * 1024;
    if (out_buf > (1 << 20)) out_buf = 1 << 20;

    if (in_each < 64 * 1024) in_each = 64 * 1024;
    if (in_each > (1 << 20)) in_each = 1 << 20;

    size_t used = out_buf + 2 * in_each;
    size_t remain = (io_budget > used) ? (io_budget - used) : 0;

    size_t part_buf = 0;
    if (P > 0) {
        part_buf = remain / (size_t)(2 * P);
        if (part_buf < 256) part_buf = 256;
        if (part_buf > 64 * 1024) part_buf = 64 * 1024;
    }

    plan.out_buf_sz  = out_buf;
    plan.inL_buf_sz  = in_each;
    plan.inR_buf_sz  = in_each;
    plan.part_buf_sz = part_buf;
    plan.total_bytes = out_buf + in_each + in_each + part_buf * (size_t)(2 * P);
    return plan;
}

#ifndef PART_PATH_LEN
#define PART_PATH_LEN 64
#endif

/* P에 따라 big_alloc로 추가로 잡히는 메타데이터(경로/포인터/카운트) 크기 */
static size_t estimate_partition_meta_bytes(unsigned int P) {
    size_t bytes = 0;
    bytes += (size_t)(2 * P) * (size_t)PART_PATH_LEN;   /* left/right paths */
    bytes += (size_t)(2 * P) * sizeof(FILE*);           /* lfp/rfp */
    bytes += (size_t)(2 * P) * sizeof(size_t);          /* left/right rec counts */
    return bytes;
}

void left_join_strategyB(const Table *left,
                         const Table *right,
                         const char *output_path,
                         int *left_block_count_out,
                         int *right_block_count_out)
{
    reset_timers_local();

    size_t B_L = left->block_size;
    size_t B_R = right->block_size;

    struct timespec tj_start, tj_end;
    clock_gettime(CLOCK_MONOTONIC, &tj_start);

    if (B_L < sizeof(BlockHeader) + sizeof(uint16_t) + MIN_REC_BYTES ||
        B_R < sizeof(BlockHeader) + sizeof(uint16_t) + MIN_REC_BYTES) {
        fprintf(stderr, "[ERROR] block_size too small: B_L=%zu, B_R=%zu\n", B_L, B_R);
        exit(EXIT_FAILURE);
    }

    int max_left_recs  = (int)(B_L / (sizeof(uint16_t) + MIN_REC_BYTES));
    int max_right_recs = (int)(B_R / (sizeof(uint16_t) + MIN_REC_BYTES));
    if (max_left_recs <= 0 || max_right_recs <= 0) {
        fprintf(stderr, "[ERROR] invalid max recs: L=%d R=%d\n", max_left_recs, max_right_recs);
        exit(EXIT_FAILURE);
    }

    struct stat stL, stR;
    size_t left_file_size  = 0;
    size_t right_file_size = 0;
    if (stat(left->filename, &stL) == 0)  left_file_size  = (size_t)stL.st_size;
    if (stat(right->filename, &stR) == 0) right_file_size = (size_t)stR.st_size;

    size_t est_left_blocks  = (left_file_size  > 0) ? ((left_file_size  + B_L - 1) / B_L) : 0;
    size_t est_right_blocks = (right_file_size > 0) ? ((right_file_size + B_R - 1) / B_R) : 0;

    /* fixed buffers (big_alloc) */
    Block *blkL = (Block*)big_alloc(B_L);
    Block *blkR = (Block*)big_alloc(B_R);
    char **recsL = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);
    char **recsR = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

    /* strict에서 안전 여유 */
    size_t safety = 64 * 1024;

    /* ===== P + I/O plan 결정 (메모리 한도까지 고려) ===== */
    unsigned int P = 1;
    IoBufPlanP io;
    memset(&io, 0, sizeof(io));

    if (right_file_size == 0) {
        P = 1;
        io = plan_iobuf_partition(g_max_memory_bytes, P);
    } else {
        /* 고정 big_alloc 사용량(현재까지) */
        size_t used_static = g_big_alloc_bytes;

        /* 반복 수렴: P -> io -> rem -> P */
        for (int it = 0; it < 8; it++) {
            IoBufPlanP trialIo = plan_iobuf_partition(g_max_memory_bytes, P);
            size_t meta_bytes = estimate_partition_meta_bytes(P);

            if (g_max_memory_bytes > 0) {
                size_t must_fit = used_static + safety + trialIo.total_bytes + meta_bytes;
                if (must_fit >= g_max_memory_bytes) {
                    fprintf(stderr,
                            "[ERROR] max_mem too small (before arena): used=%zu safety=%zu io=%zu meta=%zu limit=%zu\n",
                            used_static, safety, trialIo.total_bytes, meta_bytes, g_max_memory_bytes);
                    exit(EXIT_FAILURE);
                }
                size_t rem = g_max_memory_bytes - must_fit;

                size_t target_part_disk = rem / 2;
                if (target_part_disk < 256 * 1024) target_part_disk = 256 * 1024;

                unsigned int newP = (unsigned int)((right_file_size + target_part_disk - 1) / target_part_disk);
                if (newP < 1) newP = 1;
                newP = clamp_u32(newP, 1u, 256u);

                if (newP == P) {
                    io = trialIo;
                    break;
                }
                P = newP;
                io = trialIo;
            } else {
                /* 무제한 모드: 현재 P 유지 */
                io = trialIo;
                break;
            }
        }
    }

    /* ===== iobufs + partition 메타를 big_alloc로 먼저 확정 ===== */
    char *out_iobuf = NULL, *inL_iobuf = NULL, *inR_iobuf = NULL, *part_iobuf_pool = NULL;

    if (io.out_buf_sz) out_iobuf = (char*)big_alloc(io.out_buf_sz);
    if (io.inL_buf_sz) inL_iobuf = (char*)big_alloc(io.inL_buf_sz);
    if (io.inR_buf_sz) inR_iobuf = (char*)big_alloc(io.inR_buf_sz);

    if (io.part_buf_sz && P > 0) {
        part_iobuf_pool = (char*)big_alloc(io.part_buf_sz * (size_t)(2 * P));
    }

    /* partition arrays via big_alloc (no malloc/calloc) */
    char (*left_part_paths)[PART_PATH_LEN]  = (char(*)[PART_PATH_LEN])big_alloc((size_t)P * PART_PATH_LEN);
    char (*right_part_paths)[PART_PATH_LEN] = (char(*)[PART_PATH_LEN])big_alloc((size_t)P * PART_PATH_LEN);
    FILE **lfp = (FILE**)big_alloc(sizeof(FILE*) * (size_t)P);
    FILE **rfp = (FILE**)big_alloc(sizeof(FILE*) * (size_t)P);

    size_t *right_part_rec_counts = (size_t*)big_alloc(sizeof(size_t) * (size_t)P);
    size_t *left_part_rec_counts  = (size_t*)big_alloc(sizeof(size_t) * (size_t)P);
    memset(right_part_rec_counts, 0, sizeof(size_t) * (size_t)P);
    memset(left_part_rec_counts,  0, sizeof(size_t) * (size_t)P);

    /* ===== 이제 남은 메모리로 arena를 만들면 strict가 유지됨 ===== */
    size_t arena_cap = 0;
    if (g_max_memory_bytes > 0) {
        if (g_big_alloc_bytes + safety >= g_max_memory_bytes) {
            fprintf(stderr,
                    "[ERROR] no room for arena: used=%zu safety=%zu limit=%zu\n",
                    g_big_alloc_bytes, safety, g_max_memory_bytes);
            exit(EXIT_FAILURE);
        }
        arena_cap = g_max_memory_bytes - g_big_alloc_bytes - safety;
    } else {
        arena_cap = 128ULL << 20;
    }

    Arena arena;
    arena_init(&arena, arena_cap);

    printf("[INFO] Strategy: PARTITIONED_HASH_LEFT_JOIN (strict alloc)\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n", g_max_memory_bytes, B_L, B_R);
    printf("[INFO] max_left_recs=%d, max_right_recs=%d\n", max_left_recs, max_right_recs);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu\n", left_file_size, right_file_size);
    printf("[INFO] est_left_blocks=%zu, est_right_blocks=%zu\n", est_left_blocks, est_right_blocks);
    printf("[INFO] P=%u, fixed_big_alloc=%zu, arena_cap=%zu\n", P, g_big_alloc_bytes, arena_cap);
    printf("[INFO] iobuf: out=%zu inL=%zu inR=%zu part_each=%zu (total=%zu)\n",
           io.out_buf_sz, io.inL_buf_sz, io.inR_buf_sz, io.part_buf_sz, io.total_bytes);

    for (unsigned int p = 0; p < P; p++) {
        snprintf(left_part_paths[p],  PART_PATH_LEN, "left_part_%u.tmp",  p);
        snprintf(right_part_paths[p], PART_PATH_LEN, "right_part_%u.tmp", p);

        lfp[p] = fopen(left_part_paths[p], "w+");
        if (!lfp[p]) { perror("fopen left partition"); exit(EXIT_FAILURE); }
        rfp[p] = fopen(right_part_paths[p], "w+");
        if (!rfp[p]) { perror("fopen right partition"); exit(EXIT_FAILURE); }

        if (part_iobuf_pool && io.part_buf_sz) {
            char *bufL = part_iobuf_pool + (size_t)(2 * p) * io.part_buf_sz;
            char *bufR = part_iobuf_pool + (size_t)(2 * p + 1) * io.part_buf_sz;
            setvbuf(lfp[p], bufL, _IOFBF, io.part_buf_sz);
            setvbuf(rfp[p], bufR, _IOFBF, io.part_buf_sz);
        }
    }

    /* Phase 1: partitioning */
    int left_blocks_p1 = 0, right_blocks_p1 = 0;
    size_t left_recs_p1 = 0, right_recs_p1 = 0;

    {
        FILE *lf = fopen(left->filename, "r");
        if (!lf) { perror("fopen left"); exit(EXIT_FAILURE); }
        if (inL_iobuf) setvbuf(lf, inL_iobuf, _IOFBF, io.inL_buf_sz);

        PendingLine pend = {0};
        while (1) {
            int cnt = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(lf, B_L, blkL, recsL, &cnt, &pend, max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);
            if (!ok || cnt == 0) break;

            left_blocks_p1++;
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

                left_part_rec_counts[part]++;
                left_recs_p1++;
            }
        }
        pendingline_reset_local(&pend);
        fclose(lf);
    }

    {
        FILE *rf = fopen(right->filename, "r");
        if (!rf) { perror("fopen right"); exit(EXIT_FAILURE); }
        if (inR_iobuf) setvbuf(rf, inR_iobuf, _IOFBF, io.inR_buf_sz);

        PendingLine pend = {0};
        while (1) {
            int cnt = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rf, B_R, blkR, recsR, &cnt, &pend, max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);
            if (!ok || cnt == 0) break;

            right_blocks_p1++;
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
                right_recs_p1++;
            }
        }
        pendingline_reset_local(&pend);
        fclose(rf);
    }

    for (unsigned int p = 0; p < P; p++) {
        fflush(lfp[p]); fflush(rfp[p]);
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);
    }

    size_t nonemptyR = 0, nonemptyL = 0, maxR = 0, maxL = 0;
    for (unsigned int p = 0; p < P; p++) {
        if (right_part_rec_counts[p]) nonemptyR++;
        if (left_part_rec_counts[p])  nonemptyL++;
        if (right_part_rec_counts[p] > maxR) maxR = right_part_rec_counts[p];
        if (left_part_rec_counts[p]  > maxL) maxL = left_part_rec_counts[p];
    }

    printf("[INFO] Phase1 done: left_blocks=%d right_blocks=%d left_recs=%zu right_recs=%zu\n",
           left_blocks_p1, right_blocks_p1, left_recs_p1, right_recs_p1);
    printf("[INFO] Partition dist: nonemptyL=%zu/%u (maxL=%zu), nonemptyR=%zu/%u (maxR=%zu)\n",
           nonemptyL, P, maxL, nonemptyR, P, maxR);

    /* Phase 2: per-part join */
    FILE *out = fopen(output_path, "w");
    if (!out) { perror("fopen output"); exit(EXIT_FAILURE); }
    if (out_iobuf) setvbuf(out, out_iobuf, _IOFBF, io.out_buf_sz);

    const int nonkey_cnt = right->header.num_columns - 1;
    int left_blocks_p2 = 0, right_blocks_p2 = 0;
    size_t printed = 0;

    for (unsigned int p = 0; p < P; p++) {
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);

        if (left_part_rec_counts[p] == 0) continue;

        if (right_part_rec_counts[p] == 0) {
            PendingLine pendL = {0};
            int localLBlocks = 0;
            while (1) {
                int lc = 0;
                struct timespec tr1, tr2;
                clock_gettime(CLOCK_MONOTONIC, &tr1);
                int ok = fill_block(lfp[p], B_L, blkL, recsL, &lc, &pendL, max_left_recs);
                clock_gettime(CLOCK_MONOTONIC, &tr2);
                g_time_read += diff_sec(&tr1, &tr2);

                if (!ok || lc == 0) break;
                localLBlocks++;

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
            pendingline_reset_local(&pendL);
            left_blocks_p2 += localLBlocks;

            if (printed < 6) {
                printf("[INFO] part=%u/%u: R=0 -> emit NULLs (Lrecs=%zu)\n", p + 1, P, left_part_rec_counts[p]);
                printed++;
            }
            continue;
        }

        arena_reset(&arena);

        size_t rN = right_part_rec_counts[p];
        size_t bucket_count = next_pow2(rN * 2);
        if (bucket_count < 1024) bucket_count = 1024;

        size_t max_bucket_bytes = (arena_cap > 0) ? (arena_cap / 4) : 0;
        while (bucket_count * sizeof(HashNode*) > max_bucket_bytes && bucket_count > 1024) {
            bucket_count >>= 1;
        }

        HashNode **buckets = (HashNode**)arena_calloc_local(&arena, bucket_count, sizeof(HashNode*));
        if (!buckets) {
            fprintf(stderr, "[ERROR] arena too small for buckets: part=%u bucket_count=%zu\n", p, bucket_count);
            exit(EXIT_FAILURE);
        }

        /* build hash from right partition */
        PendingLine pendR = {0};
        int localRBlocks = 0;
        while (1) {
            int rc = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rfp[p], B_R, blkR, recsR, &rc, &pendR, max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || rc == 0) break;
            localRBlocks++;

            for (int i = 0; i < rc; i++) {
                char key_buf[256];
                get_field(recsR[i], right->header.key_index, key_buf, sizeof(key_buf));

                char nonkey_buf[4096];
                build_nonkey_fields(right, recsR[i], nonkey_buf, sizeof(nonkey_buf));

                uint64_t h = hash_string(key_buf);
                size_t idx = (size_t)(h & (bucket_count - 1));

                HashNode *node = (HashNode*)arena_alloc(&arena, sizeof(HashNode));
                if (!node) {
                    fprintf(stderr, "[ERROR] arena overflow building hash (part=%u). Increase P/max_mem.\n", p);
                    exit(EXIT_FAILURE);
                }
                node->h      = h;
                node->key    = arena_strdup(&arena, key_buf);
                node->nonkey = arena_strdup(&arena, nonkey_buf);
                if (!node->key || !node->nonkey) {
                    fprintf(stderr, "[ERROR] arena overflow on strdup (part=%u). Increase P/max_mem.\n", p);
                    exit(EXIT_FAILURE);
                }
                node->next   = buckets[idx];
                buckets[idx] = node;
            }
        }
        pendingline_reset_local(&pendR);

        /* probe with left partition */
        PendingLine pendL = {0};
        int localLBlocks = 0;
        while (1) {
            int lc = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(lfp[p], B_L, blkL, recsL, &lc, &pendL, max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || lc == 0) break;
            localLBlocks++;

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
        pendingline_reset_local(&pendL);

        left_blocks_p2  += localLBlocks;
        right_blocks_p2 += localRBlocks;

        if (printed < 10 || ((p + 1) % 32 == 0) || (p + 1 == P)) {
            printf("[INFO] part=%u/%u: Lrecs=%zu Rrecs=%zu buckets=%zu Lblocks=%d Rblocks=%d\n",
                   p + 1, P, left_part_rec_counts[p], right_part_rec_counts[p],
                   bucket_count, localLBlocks, localRBlocks);
            printed++;
        }
    }

    fclose(out);

    for (unsigned int p = 0; p < P; p++) {
        if (lfp[p]) fclose(lfp[p]);
        if (rfp[p]) fclose(rfp[p]);
        remove(left_part_paths[p]);
        remove(right_part_paths[p]);
    }

    printf("[INFO] Phase2 done: left_blocks=%d right_blocks=%d\n", left_blocks_p2, right_blocks_p2);
    printf("[INFO] SUMMARY: P=%u, phase1(L=%d R=%d), phase2(L=%d R=%d), total(L=%d R=%d)\n",
           P, left_blocks_p1, right_blocks_p1, left_blocks_p2, right_blocks_p2,
           left_blocks_p1 + left_blocks_p2, right_blocks_p1 + right_blocks_p2);

    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total = diff_sec(&tj_start, &tj_end);
    g_time_join = total - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks_p1 + left_blocks_p2;
    *right_block_count_out = right_blocks_p1 + right_blocks_p2;

    /* 파싱용 단일 라인 */
    printf("[METRIC] max_mem=%zu B_L=%zu B_R=%zu P=%u elapsed=%.6f read=%.6f join=%.6f write=%.6f\n",
           g_max_memory_bytes, B_L, B_R, P, total, g_time_read, g_time_join, g_time_write);
}
