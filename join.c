/* join.c (CHUNKED MERGE JOIN + RIGHT_RES CAN_PER_CHUNK, strict big_alloc, log-friendly)
 *
 * Merge-join logic itself is intentionally kept as-is:
 *  - left chunk load
 *  - right rescan from beginning per chunk
 *  - merge within [min_key, max_key]
 *  - right group capture (arena) + optional spill
 *
 * Only memory/block usage mechanisms are revised:
 *  - iobuf sizing depends on (max_mem, B_L, B_R)
 *  - chunk size is decided by a dedicated chunk_budget within max_mem
 *  - allocations are planned so max_mem changes affect passes clearly
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

static void reset_timers_local(void) {
    g_time_read = 0.0;
    g_time_join = 0.0;
    g_time_write = 0.0;
}

/* PendingLine 내부 구현이 프로젝트마다 다를 수 있어서:
 * 여기서는 멤버 접근/해제를 하지 않음(컴파일 에러 회피).
 * (fill_block 구현이 내부에서 동적할당을 한다면 누수가 있을 수 있음)
 */
static void pendingline_free_local(PendingLine *p) { (void)p; }

/* ------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* ------------------------------------------------------------------------- */
static size_t clamp_sz(size_t v, size_t lo, size_t hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static size_t ceil_div_sz(size_t a, size_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

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

/* ------------------------------------------------------------------------- */
/* JoinArena: one big_alloc region, reset/reuse                                */
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
/* I/O buffer plan (changed): depends on block sizes too                        */
/* ------------------------------------------------------------------------- */
typedef struct {
    size_t out_buf_sz;
    size_t inL_buf_sz;
    size_t inR_buf_sz;
    size_t spill_buf_sz;
    size_t total_bytes; /* out + inL + inR + spill */
} IoBufPlan2;

/*
 * 목적:
 *  - block_size가 크면 setvbuf도 어느 정도 커지도록(읽기/쓰기 오버헤드 감소가 더 분명히)
 *  - max_mem에서는 과도하게 커지지 않도록 caps 적용
 *
 * 정책:
 *  - 기본: inL = clamp(8*B_L, 64KB..4MB), inR = clamp(8*B_R, 64KB..4MB)
 *  - out : clamp(8*max(B_L,B_R), 64KB..4MB)
 *  - spill: clamp(4*B_R, 4KB..256KB) (tight memory면 더 줄임)
 *  - max_mem>0인 경우 io_total은 max_mem의 약 1/16을 넘지 않게 추가로 제한
 */
static IoBufPlan2 plan_iobuf2(size_t max_mem, size_t B_L, size_t B_R) {
    IoBufPlan2 p;
    memset(&p, 0, sizeof(p));

    size_t base_inL  = clamp_sz(8 * B_L, 64 * 1024, 4ULL << 20);
    size_t base_inR  = clamp_sz(8 * B_R, 64 * 1024, 4ULL << 20);
    size_t base_out  = clamp_sz(8 * (B_L > B_R ? B_L : B_R), 64 * 1024, 4ULL << 20);
    size_t base_spill = clamp_sz(4 * B_R, 4 * 1024, 256 * 1024);

    if (max_mem == 0) {
        p.inL_buf_sz  = base_inL;
        p.inR_buf_sz  = base_inR;
        p.out_buf_sz  = base_out;
        p.spill_buf_sz = base_spill;
        p.total_bytes = p.out_buf_sz + p.inL_buf_sz + p.inR_buf_sz + p.spill_buf_sz;
        return p;
    }

    /* IO 총액 제한(대략 max_mem/16, 최소 192KB, 최대 10MB) */
    size_t io_cap = max_mem / 16;
    io_cap = clamp_sz(io_cap, 192 * 1024, 10ULL << 20);

    /* 우선 base를 놓고, io_cap 넘으면 비율로 줄임 */
    size_t want = base_out + base_inL + base_inR + base_spill;
    if (want <= io_cap) {
        p.inL_buf_sz  = base_inL;
        p.inR_buf_sz  = base_inR;
        p.out_buf_sz  = base_out;
        p.spill_buf_sz = base_spill;
        p.total_bytes = want;
        return p;
    }

    /* 축소: spill 먼저 줄이고, 그 다음 in/out을 약하게 줄임 */
    size_t spill = base_spill;
    if (spill > 32 * 1024) spill = 32 * 1024;
    if (spill < 4 * 1024)  spill = 4 * 1024;

    size_t remain = (io_cap > spill) ? (io_cap - spill) : (128 * 1024);
    if (remain < 128 * 1024) remain = 128 * 1024;

    /* remain을 out:inL:inR = 2:1:1 */
    size_t out = remain / 2;
    size_t in_each = (remain - out) / 2;

    out = clamp_sz(out, 64 * 1024, 2ULL << 20);
    in_each = clamp_sz(in_each, 64 * 1024, 2ULL << 20);

    p.out_buf_sz  = out;
    p.inL_buf_sz  = in_each;
    p.inR_buf_sz  = in_each;
    p.spill_buf_sz = spill;
    p.total_bytes = p.out_buf_sz + p.inL_buf_sz + p.inR_buf_sz + p.spill_buf_sz;
    return p;
}

/* ------------------------------------------------------------------------- */
/* Right iterator (block-based)                                                */
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
/* (merge join behavior is unchanged; only memory/block plan changed)          */
/* ------------------------------------------------------------------------- */
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

    /* ---- fixed buffers (all via big_alloc) ---- */
    Block *blkR = (Block*)big_alloc(B_R);
    char  **recsR = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

    /* iobuf plan (changed) */
    IoBufPlan2 io = plan_iobuf2(g_max_memory_bytes, B_L, B_R);
    char *out_buf = (char*)big_alloc(io.out_buf_sz);
    char *inL_buf = (char*)big_alloc(io.inL_buf_sz);
    char *inR_buf = (char*)big_alloc(io.inR_buf_sz);
    char *spill_iobuf = (char*)big_alloc(io.spill_buf_sz);

    size_t safety = 64 * 1024;
    if (g_max_memory_bytes > 0 && g_big_alloc_bytes + safety >= g_max_memory_bytes) {
        fprintf(stderr, "[ERROR] max_mem too small (fixed alloc near limit): used=%zu limit=%zu\n",
                g_big_alloc_bytes, g_max_memory_bytes);
        exit(EXIT_FAILURE);
    }

    /* ---- group arena budget (small; purpose is group capture, not dominating chunk) ---- */
    size_t group_budget;
    if (g_max_memory_bytes == 0) {
        group_budget = 4ULL << 20; /* 4MB */
    } else {
        /* max_mem 변화는 chunk에 더 크게 반영되도록 group은 완만하게 증가 */
        group_budget = g_max_memory_bytes / 96; /* ~1.04% */
        group_budget = clamp_sz(group_budget, 128 * 1024, 4ULL << 20);

        /* ensure it fits */
        if (g_big_alloc_bytes + safety + group_budget >= g_max_memory_bytes) {
            size_t allow = (g_max_memory_bytes > g_big_alloc_bytes + safety + 64*1024)
                         ? (g_max_memory_bytes - g_big_alloc_bytes - safety - 64*1024)
                         : (64*1024);
            allow = clamp_sz(allow, 64*1024, 512*1024);
            group_budget = allow;
        }
    }

    JoinArena garena;
    jarena_init(&garena, group_budget);

    /* ---- decide chunk_budget and max_left_blocks_chunk (changed) ----
     * Chunk uses MOST of remaining memory so passes reflect max_mem clearly.
     */
    size_t remain_after_fixed;
    if (g_max_memory_bytes == 0) {
        /* unlimited mode: still cap chunk size to keep experiments meaningful */
        remain_after_fixed = 256ULL << 20; /* pretend 256MB budget for chunk sizing */
    } else {
        if (g_big_alloc_bytes + safety >= g_max_memory_bytes) {
            fprintf(stderr, "[ERROR] no room for chunk after fixed alloc: used=%zu limit=%zu\n",
                    g_big_alloc_bytes, g_max_memory_bytes);
            exit(EXIT_FAILURE);
        }
        remain_after_fixed = g_max_memory_bytes - g_big_alloc_bytes - safety;
    }

    /* chunk_budget: take 90% of remain (leave 10% slack for big_alloc alignment overhead etc.) */
    size_t chunk_budget = (remain_after_fixed * 9) / 10;
    if (chunk_budget < (size_t)B_L * 2) chunk_budget = (size_t)B_L * 2;

    /* bytes needed per left block stored in chunk */
    size_t per_left_block_bytes =
        B_L
        + sizeof(char*) * (size_t)max_left_recs   /* record pointer slots */
        + sizeof(int)                             /* left_counts entry */
        + sizeof(Block*);                         /* left_blks entry */

    /* add guard overhead per block to absorb allocator alignment */
    per_left_block_bytes += 64;

    size_t max_left_blocks_chunk = chunk_budget / per_left_block_bytes;
    if (max_left_blocks_chunk < 1) max_left_blocks_chunk = 1;

    /* cap to estimated left blocks to avoid allocating pointless extra */
    if (left_blocks_est > 0 && max_left_blocks_chunk > left_blocks_est) {
        max_left_blocks_chunk = left_blocks_est;
        if (max_left_blocks_chunk < 1) max_left_blocks_chunk = 1;
    }

    /* also cap to avoid pathological allocations in unlimited mode */
    if (max_left_blocks_chunk > 262144) max_left_blocks_chunk = 262144;

    size_t expected_passes = (left_blocks_est > 0)
                           ? ceil_div_sz(left_blocks_est, max_left_blocks_chunk)
                           : 1;
    if (expected_passes == 0) expected_passes = 1;

    /* ---- chunk arrays (big_alloc) ---- */
    Block **left_blks = (Block**)big_alloc(sizeof(Block*) * max_left_blocks_chunk);
    int   *left_counts = (int*)big_alloc(sizeof(int) * max_left_blocks_chunk);
    char **left_recs = (char**)big_alloc(sizeof(char*) * max_left_blocks_chunk * (size_t)max_left_recs);

    for (size_t i = 0; i < max_left_blocks_chunk; i++) {
        left_blks[i] = (Block*)big_alloc(B_L);
    }

    /* strict check after chunk alloc */
    if (g_max_memory_bytes > 0 && g_big_alloc_bytes + safety >= g_max_memory_bytes) {
        fprintf(stderr,
                "[ERROR] chunk alloc exceeded/near limit: used=%zu safety=%zu limit=%zu\n",
                g_big_alloc_bytes, safety, g_max_memory_bytes);
        exit(EXIT_FAILURE);
    }

    printf("[INFO] Strategy: CHUNKED_MERGE_JOIN + RIGHT_RESCAN_PER_CHUNK (strict join allocations)\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n", g_max_memory_bytes, B_L, B_R);
    printf("[INFO] max_left_recs=%d, max_right_recs=%d\n", max_left_recs, max_right_recs);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu\n", left_file_size, right_file_size);
    printf("[INFO] est_left_blocks=%zu, est_right_blocks=%zu\n", left_blocks_est, right_blocks_est);
    printf("[INFO] iobuf: out=%zu inL=%zu inR=%zu spill=%zu (total=%zu)\n",
           io.out_buf_sz, io.inL_buf_sz, io.inR_buf_sz, io.spill_buf_sz, io.total_bytes);
    printf("[INFO] group_budget=%zu, chunk_budget=%zu, per_left_block_bytes~=%zu\n",
           group_budget, chunk_budget, per_left_block_bytes);
    printf("[INFO] chunk_max_blocks=%zu, fixed_big_alloc=%zu\n", max_left_blocks_chunk, g_big_alloc_bytes);
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
    setvbuf(lf, inL_buf, _IOFBF, io.inL_buf_sz);

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
        setvbuf(rf, inR_buf, _IOFBF, io.inR_buf_sz);
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

        right_blocks_this_pass = rit.blocks_read;

        /* ---- merge within [min_key, max_key] ---- */
        LeftCur lc;
        lc.bi = 0; lc.ri = 0;

        char keyL[256];
        keyL[0] = 0;

        if (lc_has(&lc, chunk_blocks, left_counts)) {
            get_field(lc_get_rec(&lc, left_recs, max_left_recs), left->header.key_index, keyL, sizeof(keyL));
        }

        while (lc_has(&lc, chunk_blocks, left_counts)) {
            if (!ri_peek_key(right, &rit, keyR, sizeof(keyR))) {
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

            char *lrec0 = lc_get_rec(&lc, left_recs, max_left_recs);
            get_field(lrec0, left->header.key_index, keyL, sizeof(keyL));

            int cmp = key_compare(keyL, keyR);

            if (cmp < 0) {
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
                right_skips++;
                ri_advance(&rit);
                continue;
            }

            /* key 동일: right group 수집 후 left group과 결합 출력 */
            groups++;

            jarena_reset(&garena);

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

            char curk[256];
            snprintf(curk, sizeof(curk), "%s", keyR);

            size_t rg_total = 0;
            while (ri_peek_key(right, &rit, keyR, sizeof(keyR)) && key_compare(keyR, curk) == 0) {
                char nonkey[4096];
                build_nonkey_fields(right, ri_peek_rec(&rit), nonkey, sizeof(nonkey));

                if (!spill_mode) {
                    char *s = jarena_strdup_s(&garena, nonkey);

                    if (!rg_ptrs || !s || rg_cnt >= ptr_cap) {
                        spill_mode = 1;
                        snprintf(spill_path, sizeof(spill_path), "rg_spill_%zu_%s.tmp", chunk_passes, curk);
                        spill = fopen(spill_path, "w+");
                        if (!spill) { perror("fopen spill"); exit(EXIT_FAILURE); }
                        setvbuf(spill, spill_buf, _IOFBF, io.spill_buf_sz);

                        for (size_t i = 0; i < rg_cnt; i++) {
                            fputs(rg_ptrs[i], spill);
                            fputc('\n', spill);
                        }
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

            while (lc_has(&lc, chunk_blocks, left_counts)) {
                char *lrec = lc_get_rec(&lc, left_recs, max_left_recs);
                get_field(lrec, left->header.key_index, keyL, sizeof(keyL));
                if (key_compare(keyL, curk) != 0) break;

                if (!spill_mode) {
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

        right_blocks_this_pass = rit.blocks_read;
        right_blocks_total += (int)right_blocks_this_pass;

        pendingline_free_local(&rit.pend);
        fclose(rf);

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

    printf("[INFO] SUMMARY: chunk_passes=%zu, right_scan_passes=%zu, groups=%zu, max_right_group=%zu, left_null_rows=%zu, right_skips=%zu, Lblocks=%d, Rblocks=%d\n",
           chunk_passes, right_scan_passes, groups, max_right_group, left_null_rows, right_skips,
           left_blocks_total, right_blocks_total);

    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total = diff_sec(&tj_start, &tj_end);
    g_time_join = total - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks_total;
    *right_block_count_out = right_blocks_total;

    /* log-friendly metric line */
    printf("[METRIC] max_mem=%zu B_L=%zu B_R=%zu chunk_max_blocks=%zu passes=%zu right_scans=%zu "
           "elapsed=%.6f read=%.6f join=%.6f write=%.6f\n",
           g_max_memory_bytes, B_L, B_R, max_left_blocks_chunk, chunk_passes, right_scan_passes,
           total, g_time_read, g_time_join, g_time_write);
}
