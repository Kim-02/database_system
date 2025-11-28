/* join.c (CHUNKED MERGE JOIN + RIGHT_RES CAN_PER_CHUNK, strict big_alloc, log-friendly)
 *
 * Goal:
 *  - Memory 변화 / Block size 변화가 성능에 확실히 드러나게:
 *      => left를 "chunk" 단위로 메모리에 올리고, chunk마다 right를 처음부터 재스캔하는 방식
 *         (chunk가 작을수록 pass 수 증가 -> right 재스캔 증가 -> Read time 급증)
 *  - 메모리/블록 사용 형태는 기존 스타일 유지 (big_alloc 기반, block 기반 fill_block)
 *  - 로그 형식은 기존 형태 유지 (expected_passes / pass별 right_blocks / SUMMARY)
 *
 * Assumption:
 *  - left/right 입력은 join key 기준으로 "정렬(비내림차순)"되어 있음.
 *  - fill_block/get_field/build_nonkey_fields/Block/PendingLine 등은 기존 프로젝트 구현 사용.
 */

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

/* PendingLine 내부 구현이 프로젝트마다 다를 수 있어서:
 *  - 여기서는 멤버에 직접 접근/해제하지 않음(컴파일 에러 회피).
 *  - fill_block 내부에서 동적할당을 한다면 누수가 될 수 있지만,
 *    이 join.c는 "메모리/블록 성능 실험" 목적의 strict big_alloc 흐름을 우선.
 */
static void pendingline_free_local(PendingLine *p) {
    (void)p;
}

/* ------------------------------------------------------------------------- */
/* Small helpers: key comparison (numeric if both numeric, else strcmp)       */
/* ------------------------------------------------------------------------- */
static int is_all_digits(const char *s) {
    if (!s) return 0;
    while (*s == ' ' || *s == '\t') s++;
    if (*s == '\0') return 0;
    for (; *s; s++) {
        if (*s < '0' || *s > '9') return 0;
    }
    return 1;
}

static int key_compare(const char *a, const char *b) {
    if (a == NULL && b == NULL) return 0;
    if (a == NULL) return -1;
    if (b == NULL) return  1;

    if (is_all_digits(a) && is_all_digits(b)) {
        unsigned long long va = strtoull(a, NULL, 10);
        unsigned long long vb = strtoull(b, NULL, 10);
        if (va < vb) return -1;
        if (va > vb) return  1;
        return 0;
    }
    return strcmp(a, b);
}

/* ceil_div */
static size_t ceil_div_sz(size_t a, size_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

/* ------------------------------------------------------------------------- */
/* JoinArena: big_alloc로 한 번 잡고, 그룹 단위로 reset해서 재사용             */
/* ------------------------------------------------------------------------- */
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

static char* jarena_strdup_s(JoinArena *a, const char *s) {
    size_t n = strlen(s);
    char *p = (char*)jarena_alloc(a, n + 1, 1);
    if (!p) return NULL;
    memcpy(p, s, n + 1);
    return p;
}

/* ------------------------------------------------------------------------- */
/* I/O buffer plan (big_alloc 포함, max_mem에 따라 변화)                      */
/* ------------------------------------------------------------------------- */
typedef struct {
    size_t out_buf_sz;
    size_t in_buf_sz;
    size_t total_bytes; /* out + inL + inR */
} IoBufPlan;

static IoBufPlan plan_iobuf(size_t max_mem) {
    IoBufPlan p = {0};

    if (max_mem == 0) {
        p.out_buf_sz  = 1 << 20; /* 1MB */
        p.in_buf_sz   = 1 << 20; /* 1MB */
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

/* ------------------------------------------------------------------------- */
/* Right iterator (block-based)                                               */
/* ------------------------------------------------------------------------- */
typedef struct {
    FILE        *f;
    size_t       B;
    Block       *blk;
    char       **recs;
    int          max_recs;

    PendingLine  pend;
    int          cnt;
    int          idx;
    int          eof;

    size_t       blocks_read;
} RightIter;

static void ri_init(RightIter *it, FILE *f, size_t B, Block *blk, char **recs, int max_recs) {
    it->f = f;
    it->B = B;
    it->blk = blk;
    it->recs = recs;
    it->max_recs = max_recs;

    memset(&it->pend, 0, sizeof(it->pend));
    it->cnt = 0;
    it->idx = 0;
    it->eof = 0;
    it->blocks_read = 0;
}

static int ri_ensure(RightIter *it) {
    if (it->eof) return 0;
    if (it->idx < it->cnt) return 1;

    /* need next block */
    int rc = 0;
    struct timespec tr1, tr2;
    clock_gettime(CLOCK_MONOTONIC, &tr1);
    int ok = fill_block(it->f, it->B, it->blk, it->recs, &rc, &it->pend, it->max_recs);
    clock_gettime(CLOCK_MONOTONIC, &tr2);
    g_time_read += diff_sec(&tr1, &tr2);

    if (!ok || rc == 0) {
        it->eof = 1;
        return 0;
    }

    it->cnt = rc;
    it->idx = 0;
    it->blocks_read++;
    return 1;
}

static char* ri_peek_rec(RightIter *it) {
    if (!ri_ensure(it)) return NULL;
    return it->recs[it->idx];
}

static int ri_peek_key(const Table *right, RightIter *it, char *out, size_t cap) {
    char *r = ri_peek_rec(it);
    if (!r) return 0;
    get_field(r, right->header.key_index, out, cap);
    return 1;
}

static void ri_advance(RightIter *it) {
    if (!it->eof) it->idx++;
}

/* ------------------------------------------------------------------------- */
/* Left cursor over chunk blocks                                               */
/* ------------------------------------------------------------------------- */
typedef struct {
    int bi;
    int ri;
} LeftCur;

static int lc_has(const LeftCur *c, int chunk_blocks, const int *left_counts) {
    if (c->bi >= chunk_blocks) return 0;
    if (c->bi < 0) return 0;
    if (c->ri < left_counts[c->bi]) return 1;

    /* move to next block */
    return 0;
}

static char* lc_get_rec(const LeftCur *c, char **left_recs, int max_left_recs) {
    return left_recs[(size_t)c->bi * (size_t)max_left_recs + (size_t)c->ri];
}

static void lc_advance(LeftCur *c, int chunk_blocks, const int *left_counts) {
    c->ri++;
    while (c->bi < chunk_blocks && c->ri >= left_counts[c->bi]) {
        c->bi++;
        c->ri = 0;
    }
}

/* ------------------------------------------------------------------------- */
/* StrategyB: CHUNKED MERGE JOIN with RIGHT RESCAN PER CHUNK                   */
/* ------------------------------------------------------------------------- */
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
    size_t left_file_size = 0, right_file_size = 0;
    size_t left_blocks_est = 0, right_blocks_est = 0;

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

    /* spill file IO buffer (strict, to avoid libc hidden malloc effects in experiments) */
    size_t spill_iobuf_sz = 64 * 1024;
    if (g_max_memory_bytes > 0 && spill_iobuf_sz > g_max_memory_bytes / 16) {
        spill_iobuf_sz = 16 * 1024;
        if (spill_iobuf_sz < 4 * 1024) spill_iobuf_sz = 4 * 1024;
    }
    char *spill_iobuf = (char*)big_alloc(spill_iobuf_sz);

    /* ---- memory budgeting ---- */
    size_t safety = 64 * 1024;
    if (g_max_memory_bytes > 0 && g_big_alloc_bytes + safety > g_max_memory_bytes) {
        fprintf(stderr, "[ERROR] max_mem too small (fixed alloc already near limit)\n");
        exit(EXIT_FAILURE);
    }

    /* right group arena budget (keep small but present) */
    size_t group_budget = 0;
    if (g_max_memory_bytes == 0) {
        group_budget = 4ULL << 20; /* 4MB */
    } else {
        group_budget = g_max_memory_bytes / 64; /* 1.56% */
        if (group_budget < 128 * 1024) group_budget = 128 * 1024;
        if (group_budget > (4ULL << 20)) group_budget = (4ULL << 20);

        /* if too tight, shrink */
        if (g_big_alloc_bytes + safety + group_budget >= g_max_memory_bytes) {
            size_t allow = (g_max_memory_bytes > g_big_alloc_bytes + safety + 32*1024)
                         ? (g_max_memory_bytes - g_big_alloc_bytes - safety - 32*1024)
                         : (32*1024);
            if (allow < 32*1024) allow = 32*1024;
            group_budget = allow;
        }
    }

    JoinArena garena;
    jarena_init(&garena, group_budget);

    /* chunk size 결정: 남는 메모리 대부분을 left chunk blocks + pointer arrays에 배정 */
    size_t per_left_block_bytes_full =
        B_L
        + sizeof(char*) * (size_t)max_left_recs     /* rec pointer slots */
        + sizeof(Block*)                           /* left_blks[] entry */
        + sizeof(int);                             /* left_counts[] entry */

    size_t max_left_blocks_chunk = 1;
    if (g_max_memory_bytes > 0) {
        size_t remain = g_max_memory_bytes - g_big_alloc_bytes - safety;
        if (remain <= per_left_block_bytes_full) {
            max_left_blocks_chunk = 1;
        } else {
            max_left_blocks_chunk = remain / per_left_block_bytes_full;
            if (max_left_blocks_chunk < 1) max_left_blocks_chunk = 1;
        }
    } else {
        max_left_blocks_chunk = 65536;
    }

    size_t expected_passes = (left_blocks_est > 0)
                           ? ceil_div_sz(left_blocks_est, max_left_blocks_chunk)
                           : 0;
    if (expected_passes == 0) expected_passes = 1;

    /* ---- chunk arrays (big_alloc) ---- */
    Block **left_blks = (Block**)big_alloc(sizeof(Block*) * max_left_blocks_chunk);
    for (size_t i = 0; i < max_left_blocks_chunk; i++) {
        left_blks[i] = (Block*)big_alloc(B_L);
    }
    char **left_recs = (char**)big_alloc(sizeof(char*) * max_left_blocks_chunk * (size_t)max_left_recs);
    int  *left_counts = (int*)big_alloc(sizeof(int) * max_left_blocks_chunk);

    printf("[INFO] Strategy: CHUNKED_MERGE_JOIN + RIGHT_RESCAN_PER_CHUNK (strict join allocations)\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n", g_max_memory_bytes, B_L, B_R);
    printf("[INFO] max_left_recs=%d, max_right_recs=%d\n", max_left_recs, max_right_recs);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu\n", left_file_size, right_file_size);
    printf("[INFO] est_left_blocks=%zu, est_right_blocks=%zu\n", left_blocks_est, right_blocks_est);
    printf("[INFO] chunk_max_blocks=%zu, group_budget=%zu, fixed_big_alloc=%zu\n",
           max_left_blocks_chunk, group_budget, g_big_alloc_bytes);
    printf("[INFO] expected_passes=%zu (ceil(est_left_blocks/chunk_max_blocks))\n", expected_passes);

    int left_blocks_total = 0;
    int right_blocks_total = 0;

    size_t chunk_passes = 0;
    size_t right_scan_passes = 0;

    /* merge stats */
    size_t groups = 0;
    size_t max_right_group = 0;
    size_t left_null_rows = 0;
    size_t right_skips = 0;

    FILE *lf = fopen(left->filename, "r");
    if (!lf) { perror("fopen left"); exit(EXIT_FAILURE); }
    setvbuf(lf, inL_buf, _IOFBF, io.in_buf_sz);

    FILE *out = fopen(output_path, "w");
    if (!out) { perror("fopen output"); exit(EXIT_FAILURE); }
    setvbuf(out, out_buf, _IOFBF, io.out_buf_sz);

    PendingLine pendL = {0};
    const int nonkey_cnt = right->header.num_columns - 1;

    /* ---------------- passes ---------------- */
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
            chunk_passes--; /* 마지막 빈 패스 제외 */
            break;
        }

        /* ---- chunk record count / min_key / max_key ---- */
        int total_left_recs = 0;
        for (int bi = 0; bi < chunk_blocks; bi++) total_left_recs += left_counts[bi];

        char min_key[256], max_key[256];
        min_key[0] = 0; max_key[0] = 0;

        /* first record */
        {
            char *r0 = left_recs[0];
            get_field(r0, left->header.key_index, min_key, sizeof(min_key));
        }
        /* last record: last non-empty block */
        {
            int lbi = chunk_blocks - 1;
            int lri = left_counts[lbi] - 1;
            char *rl = left_recs[(size_t)lbi * (size_t)max_left_recs + (size_t)lri];
            get_field(rl, left->header.key_index, max_key, sizeof(max_key));
        }

        /* ---- open right from beginning (RESCAN) ---- */
        FILE *rf = fopen(right->filename, "r");
        if (!rf) { perror("fopen right"); exit(EXIT_FAILURE); }
        setvbuf(rf, inR_buf, _IOFBF, io.in_buf_sz);
        right_scan_passes++;

        RightIter rit;
        ri_init(&rit, rf, B_R, blkR, recsR, max_right_recs);

        /* skip right until key >= min_key */
        char keyR[256];
        keyR[0] = 0;
        while (ri_peek_key(right, &rit, keyR, sizeof(keyR))) {
            if (key_compare(keyR, min_key) >= 0) break;
            ri_advance(&rit);
        }

        /* block counts: include skipped reads too */
        right_blocks_this_pass = rit.blocks_read;

        /* ---- merge within [min_key, max_key] ---- */
        LeftCur lc;
        lc.bi = 0; lc.ri = 0;

        char keyL[256];
        keyL[0] = 0;

        /* load first left key */
        if (lc_has(&lc, chunk_blocks, left_counts)) {
            get_field(lc_get_rec(&lc, left_recs, max_left_recs), left->header.key_index, keyL, sizeof(keyL));
        }

        while (lc_has(&lc, chunk_blocks, left_counts)) {
            /* If right EOF -> rest left are NULL */
            if (!ri_peek_key(right, &rit, keyR, sizeof(keyR))) {
                /* output remaining left as NULL */
                while (lc_has(&lc, chunk_blocks, left_counts)) {
                    char *lrec = lc_get_rec(&lc, left_recs, max_left_recs);

                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                    fputs(lrec, out);
                    for (int k = 0; k < nonkey_cnt; k++) fputs("NULL|", out);
                    fputc('\n', out);
                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);

                    left_null_rows++;
                    lc_advance(&lc, chunk_blocks, left_counts);
                }
                break;
            }

            /* If right key already beyond max_key -> no more matches for this chunk */
            if (key_compare(keyR, max_key) > 0) {
                while (lc_has(&lc, chunk_blocks, left_counts)) {
                    char *lrec = lc_get_rec(&lc, left_recs, max_left_recs);

                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                    fputs(lrec, out);
                    for (int k = 0; k < nonkey_cnt; k++) fputs("NULL|", out);
                    fputc('\n', out);
                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);

                    left_null_rows++;
                    lc_advance(&lc, chunk_blocks, left_counts);
                }
                break;
            }

            /* current left key */
            char *lrec0 = lc_get_rec(&lc, left_recs, max_left_recs);
            get_field(lrec0, left->header.key_index, keyL, sizeof(keyL));

            int cmp = key_compare(keyL, keyR);

            if (cmp < 0) {
                /* left key < right key => left unmatched */
                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fputs(lrec0, out);
                for (int k = 0; k < nonkey_cnt; k++) fputs("NULL|", out);
                fputc('\n', out);
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);

                left_null_rows++;
                lc_advance(&lc, chunk_blocks, left_counts);
                continue;
            }

            if (cmp > 0) {
                /* right key < left key => skip right */
                right_skips++;
                ri_advance(&rit);
                continue;
            }

            /* -----------------------------------------------------------------
             * key 동일: right group 수집 후 left group과 결합 출력
             *  - group 메모리가 부족하면 spill(tmp)로 전환
             * ----------------------------------------------------------------- */
            groups++;

            jarena_reset(&garena);

            /* pointer array capacity (fit safely within arena) */
            size_t ptr_cap = garena.cap / (sizeof(char*) * 8);
            if (ptr_cap < 64) ptr_cap = 64;
            if (ptr_cap > 65536) ptr_cap = 65536;

            char **rg_ptrs = (char**)jarena_alloc(&garena, ptr_cap * sizeof(char*), sizeof(void*));
            size_t rg_cnt = 0;

            int spill_mode = 0;
            FILE *spill = NULL;
            char spill_path[128];
            spill_path[0] = '\0';
            char *spill_buf = spill_iobuf;

            /* capture current key (copy) */
            char curk[256];
            snprintf(curk, sizeof(curk), "%s", keyR);

            /* right group: consume all right rows with key==curk */
            size_t rg_total = 0;
            while (ri_peek_key(right, &rit, keyR, sizeof(keyR)) && key_compare(keyR, curk) == 0) {
                char nonkey[4096];
                build_nonkey_fields(right, ri_peek_rec(&rit), nonkey, sizeof(nonkey));

                if (!spill_mode) {
                    char *s = jarena_strdup_s(&garena, nonkey);

                    if (!rg_ptrs || !s || rg_cnt >= ptr_cap) {
                        /* switch to spill: dump what we have */
                        spill_mode = 1;
                        snprintf(spill_path, sizeof(spill_path), "rg_spill_%zu_%s.tmp", chunk_passes, curk);
                        spill = fopen(spill_path, "w+");
                        if (!spill) { perror("fopen spill"); exit(EXIT_FAILURE); }
                        setvbuf(spill, spill_buf, _IOFBF, spill_iobuf_sz);

                        /* dump already stored */
                        for (size_t i = 0; i < rg_cnt; i++) {
                            fputs(rg_ptrs[i], spill);
                            fputc('\n', spill);
                        }
                        /* dump current */
                        fputs(nonkey, spill);
                        fputc('\n', spill);
                    } else {
                        rg_ptrs[rg_cnt++] = s;
                    }
                } else {
                    fputs(nonkey, spill);
                    fputc('\n', spill);
                }

                rg_total++;
                ri_advance(&rit);
            }

            if (rg_total > max_right_group) max_right_group = rg_total;
            right_blocks_this_pass = rit.blocks_read;

            /* left group: process all left rows with key==curk */
            while (lc_has(&lc, chunk_blocks, left_counts)) {
                char *lrec = lc_get_rec(&lc, left_recs, max_left_recs);
                get_field(lrec, left->header.key_index, keyL, sizeof(keyL));
                if (key_compare(keyL, curk) != 0) break;

                if (!spill_mode) {
                    /* output all combinations from in-memory right group */
                    for (size_t i = 0; i < rg_cnt; i++) {
                        struct timespec tw1, tw2;
                        clock_gettime(CLOCK_MONOTONIC, &tw1);
                        fputs(lrec, out);
                        fputs(rg_ptrs[i], out);
                        fputc('\n', out);
                        clock_gettime(CLOCK_MONOTONIC, &tw2);
                        g_time_write += diff_sec(&tw1, &tw2);
                    }
                } else {
                    /* spill replay */
                    fflush(spill);
                    fseek(spill, 0, SEEK_SET);

                    char line[4096 + 8];
                    while (fgets(line, sizeof(line), spill)) {
                        size_t n = strlen(line);
                        if (n && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[n - 1] = '\0';

                        struct timespec tw1, tw2;
                        clock_gettime(CLOCK_MONOTONIC, &tw1);
                        fputs(lrec, out);
                        fputs(line, out);
                        fputc('\n', out);
                        clock_gettime(CLOCK_MONOTONIC, &tw2);
                        g_time_write += diff_sec(&tw1, &tw2);
                    }
                }

                lc_advance(&lc, chunk_blocks, left_counts);
            }

            if (spill_mode) {
                fclose(spill);
                remove(spill_path);
            }
        }

        /* finalize pass counters */
        right_blocks_this_pass = rit.blocks_read;
        right_blocks_total += (int)right_blocks_this_pass;

        pendingline_free_local(&rit.pend);
        fclose(rf);

        /* pass 로그(너무 많이 찍히는 것 방지: 앞 5회 + 10회마다) */
        if (chunk_passes <= 5 || (chunk_passes % 10 == 0)) {
            printf("[INFO] pass=%zu/%zu (expected~%zu), chunk_blocks=%d, left_recs=%d, key_range=[%s..%s], right_blocks_this_pass=%zu\n",
                   chunk_passes,
                   expected_passes,
                   expected_passes,
                   chunk_blocks,
                   total_left_recs,
                   min_key, max_key,
                   right_blocks_this_pass);
        }
    }

    pendingline_free_local(&pendL);
    fclose(lf);
    fclose(out);

    /* right_blocks_total은 "재스캔 기반"이므로 매우 커질 수 있음(의도된 실험 포인트) */
    printf("[INFO] SUMMARY: chunk_passes=%zu, right_scan_passes=%zu, groups=%zu, max_right_group=%zu, left_null_rows=%zu, right_skips=%zu, Lblocks=%d, Rblocks=%d\n",
           chunk_passes, right_scan_passes, groups, max_right_group, left_null_rows, right_skips,
           left_blocks_total, right_blocks_total);

    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total = diff_sec(&tj_start, &tj_end);
    g_time_join = total - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks_total;
    *right_block_count_out = right_blocks_total;
}
