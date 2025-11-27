/* join.c (CHUNKED + RIGHT_SCAN_PER_CHUNK, no malloc/calloc/strdup in join) */
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

static void pendingline_free_local(PendingLine *p) {
    if (!p) return;
    if (p->pending_line) {
        free(p->pending_line);
        p->pending_line = NULL;
    }
}

static double diff_sec(const struct timespec *s, const struct timespec *e) {
    return (e->tv_sec - s->tv_sec) + (e->tv_nsec - s->tv_nsec) / 1e9;
}

/* FNV-1a 64-bit */
static uint64_t hash_string64(const char *s) {
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

/* ============================================================
 * JoinArena: big_alloc로 한 번 잡고, chunk마다 reset만 하는 아레나
 *   - join.c 안에서는 malloc/calloc/strdup/free 금지
 * ============================================================ */
typedef struct {
    unsigned char *base;
    size_t cap;
    size_t used;
} JoinArena;

static void jarena_init(JoinArena *a, size_t cap_bytes) {
    a->base = (unsigned char*)big_alloc(cap_bytes);
    a->cap  = cap_bytes;
    a->used = 0;
}

static void jarena_reset(JoinArena *a) {
    a->used = 0;
}

static void* jarena_alloc(JoinArena *a, size_t bytes, size_t align) {
    if (align < 8) align = 8;
    size_t cur = a->used;
    size_t aligned = (cur + (align - 1)) & ~(align - 1);
    if (aligned + bytes > a->cap) return NULL;
    void *p = a->base + aligned;
    a->used = aligned + bytes;
    return p;
}

static void* jarena_calloc(JoinArena *a, size_t n, size_t sz, size_t align) {
    size_t bytes = n * sz;
    void *p = jarena_alloc(a, bytes, align);
    if (!p) return NULL;
    memset(p, 0, bytes);
    return p;
}

/* ============================================================
 * Hash table for chunked join
 *  - 노드는 (hash, left_record_ptr, matched_flag_ptr)만 저장 (문자열 복제 X)
 * ============================================================ */
typedef struct HashNode {
    uint64_t h;
    char    *left_rec;
    uint8_t *matched_flag;
    struct HashNode *next;
} HashNode;

typedef struct {
    size_t bucket_count;   /* power of 2 */
    HashNode **buckets;
} HashTable;

static int ht_init(HashTable *ht, JoinArena *a, size_t bucket_count) {
    ht->bucket_count = bucket_count;
    ht->buckets = (HashNode**)jarena_calloc(a, bucket_count, sizeof(HashNode*), sizeof(void*));
    return ht->buckets != NULL;
}

static int ht_insert(HashTable *ht, JoinArena *a, uint64_t h, char *left_rec, uint8_t *matched_flag) {
    size_t idx = (size_t)(h & (ht->bucket_count - 1));
    HashNode *n = (HashNode*)jarena_alloc(a, sizeof(HashNode), sizeof(void*));
    if (!n) return 0;
    n->h = h;
    n->left_rec = left_rec;
    n->matched_flag = matched_flag;
    n->next = ht->buckets[idx];
    ht->buckets[idx] = n;
    return 1;
}

/* I/O 버퍼도 big_alloc로 잡아서 setvbuf에 전달(= 제한에 포함) */
typedef struct {
    size_t out_buf_sz;
    size_t in_buf_sz;
    size_t total_bytes;
} IoBufPlan;

static IoBufPlan plan_iobuf(size_t max_mem) {
    IoBufPlan p = {0};

    if (max_mem == 0) {
        p.out_buf_sz = 1 << 20;   /* 1MB */
        p.in_buf_sz  = 1 << 20;   /* 1MB */
        p.total_bytes = p.out_buf_sz + 2 * p.in_buf_sz;
        return p;
    }

    /* 전체 예산의 1/32, 최소 64KB, 최대 4MB */
    size_t budget = max_mem / 32;
    if (budget < 64 * 1024) budget = 64 * 1024;
    if (budget > (4ULL << 20)) budget = (4ULL << 20);

    /* out 1/2, input(좌/우) 1/4씩 */
    p.out_buf_sz = budget / 2;
    p.in_buf_sz  = budget / 4;
    if (p.out_buf_sz < 64 * 1024) p.out_buf_sz = 64 * 1024;
    if (p.in_buf_sz  < 64 * 1024) p.in_buf_sz  = 64 * 1024;

    p.total_bytes = p.out_buf_sz + 2 * p.in_buf_sz;
    return p;
}

/* ceil_div */
static size_t ceil_div_sz(size_t a, size_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

/* ============================================================
 * StrategyB:
 *   - (2) LEFT_CHUNKED + RIGHT_SCAN_PER_CHUNK (네가 원하는 구조)
 *   - (log) expected_passes / pass별 right blocks / 총 pass 요약
 * ============================================================ */
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

    /* ---- basic checks ---- */
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

    /* ---- file size estimation (for logs) ---- */
    struct stat stL, stR;
    size_t left_file_size = 0;
    size_t right_file_size = 0;
    size_t left_blocks_est = 0;
    size_t right_blocks_est = 0;

    if (stat(left->filename, &stL) == 0) {
        left_file_size = (size_t)stL.st_size;
        left_blocks_est = (left_file_size + B_L - 1) / B_L;
        if (left_blocks_est == 0) left_blocks_est = 1;
    }
    if (stat(right->filename, &stR) == 0) {
        right_file_size = (size_t)stR.st_size;
        right_blocks_est = (right_file_size + B_R - 1) / B_R;
        if (right_blocks_est == 0) right_blocks_est = 1;
    }

    /* ---- allocate fixed buffers (all via big_alloc) ---- */
    Block *blkR = (Block*)big_alloc(B_R);
    char **recsR = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

    IoBufPlan io = plan_iobuf(g_max_memory_bytes);
    char *out_buf = (char*)big_alloc(io.out_buf_sz);
    char *inL_buf = (char*)big_alloc(io.in_buf_sz);
    char *inR_buf = (char*)big_alloc(io.in_buf_sz);

    /* ---- decide chunk budget vs hash budget (strict within max_mem) ---- */
    size_t safety = 64 * 1024;

    if (g_max_memory_bytes > 0 && g_big_alloc_bytes + safety > g_max_memory_bytes) {
        fprintf(stderr, "[ERROR] max_mem too small (fixed alloc already near limit)\n");
        exit(EXIT_FAILURE);
    }

    /* 해시 아레나 예산: max_mem의 1/8, 최소 256KB, 최대 max_mem/2 */
    size_t hash_budget = 0;
    if (g_max_memory_bytes == 0) {
        hash_budget = 64ULL << 20; /* 64MB default when unlimited */
    } else {
        hash_budget = g_max_memory_bytes / 8;
        if (hash_budget < 256 * 1024) hash_budget = 256 * 1024;
        if (hash_budget > g_max_memory_bytes / 2) hash_budget = g_max_memory_bytes / 2;
    }

    /* 오른쪽 1블록은 항상 있어야 함 */
    size_t per_right_block_bytes =
        B_R + sizeof(char*) * (size_t)max_right_recs;

    size_t per_left_block_bytes_full =
        B_L + sizeof(char*) * (size_t)max_left_recs + sizeof(Block*) + sizeof(int);

    size_t max_left_blocks_chunk = 1;
    if (g_max_memory_bytes > 0) {
        size_t used_now = g_big_alloc_bytes;
        if (used_now + safety + hash_budget >= g_max_memory_bytes) {
            /* 해시 예산이 너무 커서 chunk가 0이 되는 상황 방지 */
            hash_budget = (g_max_memory_bytes > used_now + safety + 64*1024)
                        ? (g_max_memory_bytes - used_now - safety - 64*1024)
                        : 64*1024;
        }

        size_t remain = g_max_memory_bytes - g_big_alloc_bytes - safety;
        /* remain 안에서: hash_budget + (right 1blk) + (left chunk) */
        if (remain <= hash_budget + per_right_block_bytes + per_left_block_bytes_full) {
            max_left_blocks_chunk = 1;
        } else {
            size_t left_budget = remain - hash_budget - per_right_block_bytes;
            max_left_blocks_chunk = left_budget / per_left_block_bytes_full;
            if (max_left_blocks_chunk < 1) max_left_blocks_chunk = 1;
        }
    } else {
        max_left_blocks_chunk = 65536; /* arbitrary cap when unlimited */
    }

    /* expected passes log (left_blocks_est / chunk_max_blocks) */
    size_t expected_passes = 0;
    if (left_blocks_est > 0) {
        expected_passes = ceil_div_sz(left_blocks_est, max_left_blocks_chunk);
        if (expected_passes == 0) expected_passes = 1;
    }

    /* chunk arrays */
    Block **left_blks = (Block**)big_alloc(sizeof(Block*) * max_left_blocks_chunk);
    for (size_t i = 0; i < max_left_blocks_chunk; i++) {
        left_blks[i] = (Block*)big_alloc(B_L);
    }
    char **left_recs = (char**)big_alloc(sizeof(char*) * max_left_blocks_chunk * (size_t)max_left_recs);
    int  *left_counts = (int*)big_alloc(sizeof(int) * max_left_blocks_chunk);

    /* right scan buffers (1 block) */
    Block *right_blk = blkR;
    char **right_recs = recsR;

    /* hash arena */
    JoinArena arena;
    jarena_init(&arena, hash_budget);

    printf("[INFO] Strategy: LEFT_CHUNKED + RIGHT_SCAN_PER_CHUNK (strict join allocations)\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n", g_max_memory_bytes, B_L, B_R);
    printf("[INFO] max_left_recs=%d, max_right_recs=%d\n", max_left_recs, max_right_recs);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu\n", left_file_size, right_file_size);
    printf("[INFO] est_left_blocks=%zu, est_right_blocks=%zu\n", left_blocks_est, right_blocks_est);
    printf("[INFO] chunk_max_blocks=%zu, hash_budget=%zu, fixed_big_alloc=%zu\n",
           max_left_blocks_chunk, hash_budget, g_big_alloc_bytes);
    printf("[INFO] expected_passes=%zu (ceil(est_left_blocks/chunk_max_blocks))\n", expected_passes);

    int left_blocks_total = 0;
    int right_blocks_total = 0;

    /* pass counters (LOG 핵심) */
    size_t chunk_passes = 0;
    size_t right_scan_passes = 0;

    FILE *lf = fopen(left->filename, "r");
    if (!lf) { perror("fopen left"); exit(EXIT_FAILURE); }
    setvbuf(lf, inL_buf, _IOFBF, io.in_buf_sz);

    FILE *out = fopen(output_path, "w");
    if (!out) { perror("fopen output"); exit(EXIT_FAILURE); }
    setvbuf(out, out_buf, _IOFBF, io.out_buf_sz);

    PendingLine pendL = {0};
    const int nonkey_cnt = right->header.num_columns - 1;

    while (1) {
        chunk_passes++;
        size_t right_blocks_this_pass = 0;

        /* ---- load one left chunk ---- */
        int chunk_blocks = 0;

        for (size_t bi = 0; bi < max_left_blocks_chunk; bi++) {
            int lc = 0;
            char **rec_ptrs_for_block = &left_recs[bi * (size_t)max_left_recs];

            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(lf, B_L, left_blks[bi], rec_ptrs_for_block, &lc, &pendL, max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || lc == 0) break;

            left_counts[bi] = lc;
            chunk_blocks++;
            left_blocks_total++;
        }

        if (chunk_blocks == 0) {
            chunk_passes--; /* 마지막 빈 패스는 제외 */
            break;
        }

        /* ---- build flat matched bitmap for this chunk (in arena) ---- */
        int total_left_recs = 0;
        for (int bi = 0; bi < chunk_blocks; bi++) total_left_recs += left_counts[bi];

        jarena_reset(&arena);

        uint8_t *matched = (uint8_t*)jarena_calloc(&arena, (size_t)total_left_recs, sizeof(uint8_t), 8);
        if (!matched) {
            fprintf(stderr, "[ERROR] hash arena too small for matched bitmap (%d recs)\n", total_left_recs);
            exit(EXIT_FAILURE);
        }

        /* ---- init hash table ---- */
        size_t bucket_count = next_pow2((size_t)total_left_recs * 2);
        if (bucket_count < 1024) bucket_count = 1024;

        /* buckets too big? shrink */
        while (bucket_count * sizeof(HashNode*) > arena.cap / 3 && bucket_count > 1024) {
            bucket_count >>= 1;
        }

        HashTable ht;
        if (!ht_init(&ht, &arena, bucket_count)) {
            fprintf(stderr, "[ERROR] hash arena too small for buckets (%zu)\n", bucket_count);
            exit(EXIT_FAILURE);
        }

        /* ---- insert all left records into hash table ---- */
        int rid = 0;
        for (int bi = 0; bi < chunk_blocks; bi++) {
            char **blk_recs = &left_recs[(size_t)bi * (size_t)max_left_recs];
            for (int i = 0; i < left_counts[bi]; i++) {
                char keyL[256];
                get_field(blk_recs[i], left->header.key_index, keyL, sizeof(keyL));
                uint64_t h = hash_string64(keyL);

                if (!ht_insert(&ht, &arena, h, blk_recs[i], &matched[rid])) {
                    fprintf(stderr, "[ERROR] hash arena overflow while inserting left records\n");
                    exit(EXIT_FAILURE);
                }
                rid++;
            }
        }

        /* ---- scan right once for this chunk ---- */
        FILE *rf = fopen(right->filename, "r");
        if (!rf) { perror("fopen right"); exit(EXIT_FAILURE); }
        setvbuf(rf, inR_buf, _IOFBF, io.in_buf_sz);

        right_scan_passes++;

        PendingLine pendR = {0};

        while (1) {
            int rc = 0;

            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rf, B_R, right_blk, right_recs, &rc, &pendR, max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || rc == 0) break;

            right_blocks_total++;
            right_blocks_this_pass++;

            for (int j = 0; j < rc; j++) {
                char keyR[256];
                get_field(right_recs[j], right->header.key_index, keyR, sizeof(keyR));
                uint64_t h = hash_string64(keyR);
                size_t b = (size_t)(h & (ht.bucket_count - 1));

                /* probe chain */
                for (HashNode *n = ht.buckets[b]; n; n = n->next) {
                    if (n->h != h) continue;

                    /* collision check */
                    char keyL2[256];
                    get_field(n->left_rec, left->header.key_index, keyL2, sizeof(keyL2));
                    if (strcmp(keyL2, keyR) != 0) continue;

                    char right_nonkey[4096];
                    build_nonkey_fields(right, right_recs[j], right_nonkey, sizeof(right_nonkey));

                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                    fputs(n->left_rec, out);
                    fputs(right_nonkey, out);
                    fputc('\n', out);
                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);

                    *(n->matched_flag) = 1;
                }
            }
        }

        /* PendingLine이 내부에서 malloc을 쓴다면 누수 방지 */
        pendingline_free_local(&pendR);
        fclose(rf);

        /* pass 로그(너무 많이 찍히는 것 방지: 앞 5회 + 10회마다) */
        if (chunk_passes <= 5 || (chunk_passes % 10 == 0)) {
            printf("[INFO] pass=%zu/%zu (expected~%zu), chunk_blocks=%d, left_recs=%d, right_blocks_this_pass=%zu\n",
                   chunk_passes,
                   (expected_passes ? expected_passes : 0),
                   expected_passes,
                   chunk_blocks,
                   total_left_recs,
                   right_blocks_this_pass);
        }

        /* ---- emit unmatched left records with NULLs ---- */
        rid = 0;
        for (int bi = 0; bi < chunk_blocks; bi++) {
            char **blk_recs = &left_recs[(size_t)bi * (size_t)max_left_recs];
            for (int i = 0; i < left_counts[bi]; i++) {
                if (matched[rid] == 0) {
                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);

                    fputs(blk_recs[i], out);
                    for (int k = 0; k < nonkey_cnt; k++) fputs("NULL|", out);
                    fputc('\n', out);

                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);
                }
                rid++;
            }
        }
    }

    pendingline_free_local(&pendL);
    fclose(lf);
    fclose(out);

    /* 최종 요약 로그 */
    printf("[INFO] SUMMARY: chunk_passes=%zu, right_scan_passes=%zu, total_left_blocks=%d, total_right_blocks=%d\n",
           chunk_passes, right_scan_passes, left_blocks_total, right_blocks_total);

    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total = diff_sec(&tj_start, &tj_end);
    g_time_join = total - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks_total;
    *right_block_count_out = right_blocks_total;
}
