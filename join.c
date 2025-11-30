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

/* 시간 차이 계산 */
static double diff_sec(const struct timespec *s, const struct timespec *e) {
    return (e->tv_sec - s->tv_sec) + (e->tv_nsec - s->tv_nsec) / 1e9;
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

static void reset_timers_local(void) {
    g_time_read = g_time_join = g_time_write = 0.0;
}

/* hash node stored in arena */
typedef struct HashNode {
    uint64_t        h;
    char           *key;
    char           *nonkey;
    struct HashNode *next;
} HashNode;

/* I/O buffer plan */
typedef struct IOPlan {
    size_t out_buf_sz;
    size_t inL_buf_sz;
    size_t inR_buf_sz;
    size_t part_buf_sz;   /* per-partition-file buffer size (one for left, one for right → 2*P개) */
    size_t total_bytes;   /* out+inL+inR + (part_buf_sz * 2 * P) */
} IOPlan;

/* arena calloc helper */
static void *arena_calloc(Arena *a, size_t n, size_t sz) {
    size_t bytes = n * sz;
    void *p = arena_alloc(a, bytes);
    if (!p) return NULL;
    memset(p, 0, bytes);
    return p;
}

/* 고정 상수 */
#ifndef PART_PATH_LEN
#define PART_PATH_LEN 256
#endif

#ifndef MIN_REC_BYTES
#define MIN_REC_BYTES 2
#endif

/* P에 따라 big_alloc로 추가로 잡히는 메타데이터(경로/포인터/카운트) 크기 */
static size_t estimate_partition_meta_bytes(unsigned int P) {
    size_t bytes = 0;
    bytes += (size_t)(2 * P) * (size_t)PART_PATH_LEN;   /* left/right paths */
    bytes += (size_t)(2 * P) * sizeof(FILE*);           /* lfp/rfp */
    bytes += (size_t)(2 * P) * sizeof(size_t);          /* left/right rec counts */
    return bytes;
}

/* I/O 버퍼 크기 결정: 이미 big_alloc으로 잡힌 용량(g_big_alloc_bytes)와
   partition 메타(meta_bytes), safety 등을 고려해서 out/inL/inR/part_buf 크기를 정함 */
static IOPlan choose_ioplan(size_t max_mem,
                            size_t used_so_far,
                            size_t meta_bytes,
                            size_t safety,
                            unsigned int P)
{
    IOPlan plan;
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

    size_t per_stream = io_budget / 4;
    if (per_stream < 64 * 1024) per_stream = 64 * 1024;

    plan.out_buf_sz  = per_stream;
    plan.inL_buf_sz  = per_stream;
    plan.inR_buf_sz  = per_stream;

    size_t part_budget = io_budget / 4;
    size_t part_each   = (P > 0) ? (part_budget / (2 * (size_t)P)) : (16 * 1024);
    if (part_each < 16 * 1024) part_each = 16 * 1024;

    plan.part_buf_sz = part_each;

    /* HARD CAP: 너무 크게 잡히지 않도록 상한 */
    const size_t MAX_STREAM_BUF = 1ULL << 20;   /* 1MB */
    const size_t MAX_PART_BUF   = 256ULL << 10; /* 256KB */

    if (plan.out_buf_sz  > MAX_STREAM_BUF) plan.out_buf_sz  = MAX_STREAM_BUF;
    if (plan.inL_buf_sz  > MAX_STREAM_BUF) plan.inL_buf_sz  = MAX_STREAM_BUF;
    if (plan.inR_buf_sz  > MAX_STREAM_BUF) plan.inR_buf_sz  = MAX_STREAM_BUF;
    if (plan.part_buf_sz > MAX_PART_BUF)   plan.part_buf_sz= MAX_PART_BUF;

    /* 최소 보장 */
    if (plan.out_buf_sz  < 64 * 1024)  plan.out_buf_sz  = 64 * 1024;
    if (plan.inL_buf_sz  < 64 * 1024)  plan.inL_buf_sz  = 64 * 1024;
    if (plan.inR_buf_sz  < 64 * 1024)  plan.inR_buf_sz  = 64 * 1024;
    if (plan.part_buf_sz < 16 * 1024)  plan.part_buf_sz = 16 * 1024;

    plan.total_bytes = plan.out_buf_sz + plan.inL_buf_sz + plan.inR_buf_sz
                     + plan.part_buf_sz * (size_t)(2 * P);

    return plan;
}

/* 메인 조인 함수 */
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
        fprintf(stderr,
                "[ERROR] block size too small. Need header + at least one record.\n");
        exit(EXIT_FAILURE);
    }

    /* === 1. 기본 블록/레코드 포인터 배열을 big_alloc으로 잡음 === */
    int max_left_recs  = (int)(B_L / (size_t)(sizeof(uint16_t) + MIN_REC_BYTES));
    int max_right_recs = (int)(B_R / (size_t)(sizeof(uint16_t) + MIN_REC_BYTES));
    if (max_left_recs  < 64) max_left_recs  = 64;
    if (max_right_recs < 64) max_right_recs = 64;

    Block *blkL = (Block*)big_alloc(B_L);
    Block *blkR = (Block*)big_alloc(B_R);

    char **recsL = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);
    char **recsR = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

    struct stat stL, stR;
    if (stat(left->path, &stL) != 0) {
        perror("stat left");
        exit(EXIT_FAILURE);
    }
    if (stat(right->path, &stR) != 0) {
        perror("stat right");
        exit(EXIT_FAILURE);
    }
    size_t left_file_size  = (size_t)stL.st_size;
    size_t right_file_size = (size_t)stR.st_size;

    size_t est_left_blocks  = (left_file_size  + B_L - 1) / B_L;
    size_t est_right_blocks = (right_file_size + B_R - 1) / B_R;

    /* === 2. 파티션 수 P 선택 === */
    unsigned int P = 8;
    size_t meta_bytes = estimate_partition_meta_bytes(P);

    const size_t safety = 8ULL << 20;  /* 8MB 여유 */

    IOPlan io = choose_ioplan(g_max_memory_bytes,
                              g_big_alloc_bytes + meta_bytes,
                              meta_bytes,
                              safety,
                              P);

    if (g_max_memory_bytes > 0) {
        size_t must = g_big_alloc_bytes + meta_bytes + io.total_bytes + safety;
        if (must > g_max_memory_bytes) {
            /* 파티션 수를 늘려 파티션당 파일 크기를 줄이는 방향 등,
               여기서는 단순히 에러 처리 */
            fprintf(stderr,
                    "[ERROR] Not enough memory even for iobuf+meta: need=%zu limit=%zu\n",
                    must, g_max_memory_bytes);
            exit(EXIT_FAILURE);
        }
    }

    /* arena 용량: 남은 메모리 전부를 arena로 사용 (없으면 128MB) */
    size_t arena_cap = 0;
    if (g_max_memory_bytes > 0) {
        if (g_big_alloc_bytes + meta_bytes + io.total_bytes + safety >= g_max_memory_bytes) {
            fprintf(stderr,
                    "[ERROR] No room for arena: used=%zu meta=%zu io=%zu safety=%zu limit=%zu\n",
                    g_big_alloc_bytes, meta_bytes, io.total_bytes, safety, g_max_memory_bytes);
            exit(EXIT_FAILURE);
        }
        arena_cap = g_max_memory_bytes - g_big_alloc_bytes - meta_bytes - io.total_bytes - safety;
        if (arena_cap < (8ULL << 20)) {
            fprintf(stderr,
                    "[WARN] arena_cap is small (%zu bytes). Joins may fail for large partitions.\n",
                    arena_cap);
        }
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
    printf("[INFO] P=%u, fixed_big_alloc=%zu, arena_cap=%zu\n",
           P, g_big_alloc_bytes, arena_cap);
    printf("[INFO] iobuf: out=%zu inL=%zu inR=%zu part_each=%zu (total=%zu)\n",
           io.out_buf_sz, io.inL_buf_sz, io.inR_buf_sz,
           io.part_buf_sz, io.total_bytes);

    /* === 3. I/O 버퍼 / 파티션 파일 정보 big_alloc === */
    char *out_iobuf = (char*)big_alloc(io.out_buf_sz);
    char *inL_iobuf = (char*)big_alloc(io.inL_buf_sz);
    char *inR_iobuf = (char*)big_alloc(io.inR_buf_sz);

    char (*left_part_paths)[PART_PATH_LEN] =
        (char(*)[PART_PATH_LEN])big_alloc((size_t)P * (size_t)PART_PATH_LEN);
    char (*right_part_paths)[PART_PATH_LEN] =
        (char(*)[PART_PATH_LEN])big_alloc((size_t)P * (size_t)PART_PATH_LEN);

    FILE **lfp = (FILE**)big_alloc(sizeof(FILE*) * (size_t)P);
    FILE **rfp = (FILE**)big_alloc(sizeof(FILE*) * (size_t)P);

    size_t *left_part_rec_counts  = (size_t*)big_alloc(sizeof(size_t) * (size_t)P);
    size_t *right_part_rec_counts = (size_t*)big_alloc(sizeof(size_t) * (size_t)P);

    memset(left_part_rec_counts,  0, sizeof(size_t) * (size_t)P);
    memset(right_part_rec_counts, 0, sizeof(size_t) * (size_t)P);

    /* 파티션 파일용 버퍼 풀: (left,right) * P 개 */
    char *part_iobuf_pool = NULL;
    if (io.part_buf_sz > 0) {
        part_iobuf_pool =
            (char*)big_alloc(io.part_buf_sz * (size_t)(2 * P));
    }

    /* === 4. 입력 파일 오픈 및 버퍼 설정 === */
    FILE *lf = fopen(left->path, "r");
    if (!lf) {
        perror("fopen left");
        exit(EXIT_FAILURE);
    }
    FILE *rf = fopen(right->path, "r");
    if (!rf) {
        perror("fopen right");
        exit(EXIT_FAILURE);
    }

    if (inL_iobuf && io.inL_buf_sz)
        setvbuf(lf, inL_iobuf, _IOFBF, io.inL_buf_sz);
    if (inR_iobuf && io.inR_buf_sz)
        setvbuf(rf, inR_iobuf, _IOFBF, io.inR_buf_sz);

    /* 파티션 파일 생성 */
    for (unsigned int p = 0; p < P; p++) {
        snprintf(left_part_paths[p],  PART_PATH_LEN, "left_part_%u.tmp",  p);
        snprintf(right_part_paths[p], PART_PATH_LEN, "right_part_%u.tmp", p);

        lfp[p] = fopen(left_part_paths[p], "w+");
        if (!lfp[p]) {
            perror("fopen left partition");
            exit(EXIT_FAILURE);
        }
        rfp[p] = fopen(right_part_paths[p], "w+");
        if (!rfp[p]) {
            perror("fopen right partition");
            exit(EXIT_FAILURE);
        }

        if (part_iobuf_pool && io.part_buf_sz) {
            char *bufL = part_iobuf_pool + (size_t)(2 * p) * io.part_buf_sz;
            char *bufR = part_iobuf_pool + (size_t)(2 * p + 1) * io.part_buf_sz;
            setvbuf(lfp[p], bufL, _IOFBF, io.part_buf_sz);
            setvbuf(rfp[p], bufR, _IOFBF, io.part_buf_sz);
        }
    }

    /* === 5. Phase1: partitioning === */
    int left_blocks_p1  = 0;
    int right_blocks_p1 = 0;
    size_t left_recs_p1 = 0;
    size_t right_recs_p1= 0;

    PendingLine pend = {0};

    /* left partition */
    while (1) {
        int lc = 0;
        struct timespec tr1, tr2;
        clock_gettime(CLOCK_MONOTONIC, &tr1);
        int ok = fill_block(lf, B_L, blkL, recsL, &lc, &pend, max_left_recs);
        clock_gettime(CLOCK_MONOTONIC, &tr2);
        g_time_read += diff_sec(&tr1, &tr2);

        if (!ok || lc == 0) break;
        left_blocks_p1++;

        for (int i = 0; i < lc; i++) {
            char key_buf[256];
            get_field(recsL[i], left->header.key_index, key_buf, sizeof(key_buf));

            uint64_t h = hash_string(key_buf);
            unsigned int part = (unsigned int)(h % P);

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

    for (unsigned int p = 0; p < P; p++) {
        fflush(lfp[p]);
        fseek(lfp[p], 0, SEEK_SET);
    }
    fclose(lf);

    /* right partition */
    pend.pending_len = 0;
    while (1) {
        int rc = 0;
        struct timespec tr1, tr2;
        clock_gettime(CLOCK_MONOTONIC, &tr1);
        int ok = fill_block(rf, B_R, blkR, recsR, &rc, &pend, max_right_recs);
        clock_gettime(CLOCK_MONOTONIC, &tr2);
        g_time_read += diff_sec(&tr1, &tr2);

        if (!ok || rc == 0) break;
        right_blocks_p1++;

        for (int i = 0; i < rc; i++) {
            char key_buf[256];
            get_field(recsR[i], right->header.key_index, key_buf, sizeof(key_buf));

            uint64_t h = hash_string(key_buf);
            unsigned int part = (unsigned int)(h % P);

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

    for (unsigned int p = 0; p < P; p++) {
        fflush(rfp[p]);
        fseek(rfp[p], 0, SEEK_SET);
    }

    size_t nonemptyL = 0, nonemptyR = 0, maxL = 0, maxR = 0;
    for (unsigned int p = 0; p < P; p++) {
        if (left_part_rec_counts[p])  nonemptyL++;
        if (right_part_rec_counts[p]) nonemptyR++;
        if (left_part_rec_counts[p]  > maxL) maxL = left_part_rec_counts[p];
        if (right_part_rec_counts[p] > maxR) maxR = right_part_rec_counts[p];
    }

    printf("[INFO] Phase1 done: left_blocks=%d right_blocks=%d left_recs=%zu right_recs=%zu\n",
           left_blocks_p1, right_blocks_p1, left_recs_p1, right_recs_p1);
    printf("[INFO] Partition dist: nonemptyL=%zu/%u (maxL=%zu), nonemptyR=%zu/%u (maxR=%zu)\n",
           nonemptyL, P, maxL, nonemptyR, P, maxR);

    /* === 6. Phase2: 파티션별 조인 (메모리는 기존 방식, probe만 OMP 병렬) === */
    const int nonkey_cnt = right->header.num_columns - 1;
    int left_blocks_p2  = 0;
    int right_blocks_p2 = 0;

    FILE *out = fopen(output_path, "w");
    if (!out) {
        perror("fopen output");
        exit(EXIT_FAILURE);
    }
    if (out_iobuf && io.out_buf_sz)
        setvbuf(out, out_iobuf, _IOFBF, io.out_buf_sz);

    size_t printed = 0;

    for (unsigned int p = 0; p < P; p++) {
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);

        if (left_part_rec_counts[p] == 0) {
            continue;
        }
        if (right_part_rec_counts[p] == 0) {
            /* 오른쪽 파티션이 비었으면, 왼쪽만 NULL로 채워서 출력 */
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
                printf("[INFO] part=%u/%u: R=0 -> emit NULLs (Lrecs=%zu)\n",
                       p + 1, P, left_part_rec_counts[p]);
                printed++;
            }
            continue;
        }

        /* 해시테이블 새로 구축 */
        arena_reset(&arena);

        size_t rN = right_part_rec_counts[p];

        /* bucket_count: load factor ~ 1.0 (strict mem에서 버킷 오버헤드 줄이기) */
        size_t bucket_count = next_pow2(rN);
        if (bucket_count < 1024) bucket_count = 1024;

        /* 버킷이 arena를 너무 많이 먹지 않도록 상한(12.5%) */
        size_t max_bucket_bytes = (arena_cap > 0) ? (arena_cap / 8) : 0;
        if (max_bucket_bytes > 0) {
            while (bucket_count * sizeof(HashNode*) > max_bucket_bytes && bucket_count > 1024) {
                bucket_count >>= 1;
            }
        }
        HashNode **buckets = (HashNode**)arena_calloc(&arena, bucket_count, sizeof(HashNode*));
        if (!buckets) {
            fprintf(stderr,
                    "[ERROR] arena too small for buckets: part=%u bucket_count=%zu cap=%zu\n",
                    p, bucket_count, arena_cap);
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
                    fprintf(stderr,
                            "[ERROR] arena overflow building hash (part=%u). Increase P/max_mem.\n",
                            p);
                    exit(EXIT_FAILURE);
                }
                node->h      = h;
                node->key    = arena_strdup(&arena, key_buf);
                node->nonkey = arena_strdup(&arena, nonkey_buf);
                if (!node->key || !node->nonkey) {
                    fprintf(stderr,
                            "[ERROR] arena overflow on strdup (part=%u). Increase P/max_mem.\n",
                            p);
                    exit(EXIT_FAILURE);
                }
                node->next   = buckets[idx];
                buckets[idx] = node;
            }
        }
        pendingline_reset_local(&pendR);

        /* probe with left partition (레코드 루프만 OpenMP 병렬) */
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

            /* ==== probe left block with OpenMP over records ==== */
#ifdef _OPENMP
#pragma omp parallel for schedule(static)
#endif
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
                        /* 출력은 파일 공유 자원이라 임계영역에서 처리 */
                    #pragma omp critical(join_out_write)
                        {
                            struct timespec tw1, tw2;
                            clock_gettime(CLOCK_MONOTONIC, &tw1);

                            fputs(recsL[i], out);
                            fputs(n->nonkey, out);
                            fputc('\n', out);

                            clock_gettime(CLOCK_MONOTONIC, &tw2);
                            g_time_write += diff_sec(&tw1, &tw2);
                        }
                    }
                }

                if (!matched) {
                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                #pragma omp critical(join_out_write)
                {
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
        }
        pendingline_reset_local(&pendL);

        left_blocks_p2  += localLBlocks;
        right_blocks_p2 += localRBlocks;

        if (printed < 10 || ((p + 1) % 16 == 0) || (p + 1 == P)) {
            printf("[INFO] part=%u/%u: Lrecs=%zu Rrecs=%zu buckets=%zu Lblocks=%d Rblocks=%d\n",
                   p + 1, P,
                   left_part_rec_counts[p], right_part_rec_counts[p],
                   bucket_count, localLBlocks, localRBlocks);
            printed++;
        }
    }

    fclose(out);

    /* partition 파일 정리 */
    for (unsigned int p = 0; p < P; p++) {
        if (lfp[p]) fclose(lfp[p]);
        if (rfp[p]) fclose(rfp[p]);
        remove(left_part_paths[p]);
        remove(right_part_paths[p]);
    }

    printf("[INFO] Phase2 done: left_blocks=%d right_blocks=%d\n",
           left_blocks_p2, right_blocks_p2);

    printf("[INFO] SUMMARY: P=%u, phase1(L=%d R=%d), phase2(L=%d R=%d), total(L=%d R=%d)\n",
           P, left_blocks_p1, right_blocks_p1,
           left_blocks_p2, right_blocks_p2,
           left_blocks_p1 + left_blocks_p2,
           right_blocks_p1 + right_blocks_p2);

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
