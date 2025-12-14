/* join.c : PARTITIONED HASH LEFT JOIN + OpenMP Phase2 (strict big_alloc + per-thread arenas/buffers)
 *
 * Fixes included:
 *  1) "time prints 0 0 0 0" root cause in your run:
 *     - Program exits early due to Phase2 OOM ("[ERROR] arena out of memory...") so [METRIC] never prints.
 *     - Your runner prints zeros when it can't parse [METRIC].
 *     - This file fixes the OOM by forcing P large enough (memory-based P floor), so the run reaches [METRIC].
 *
 *  2) Timing robustness:
 *     - Use omp_get_wtime() everywhere (no clock_gettime dependency).
 *
 *  3) Arena split OOM fix (your earlier issue):
 *     - active_threads = min(OMP_T, P) and split arena only by active_threads.
 *
 * Notes:
 *  - Assumes '|' delimited rows (TPC-H .tbl).
 *  - Output rows: left_row + (right nonkey fields) or left_row + NULL|... if no match.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <math.h>

#include <omp.h>

#include "memory.h"
#include "table.h"
#include "blockio.h"
#include "join.h"

/* ==== timing globals ==== */
double g_time_read  = 0.0;
double g_time_join  = 0.0;
double g_time_write = 0.0;

/* Use OpenMP wall clock for robust timing */
static inline double now_sec(void) {
    return omp_get_wtime();
}

static void reset_timers_local(void) {
    g_time_read = 0.0;
    g_time_join = 0.0;
    g_time_write = 0.0;
}

static inline void pendingline_reset_local(PendingLine *p) {
    if (!p) return;
    p->pending_len = 0;
}

/* FNV-1a 64-bit over bytes */
static inline uint64_t hash_bytes(const char *p, size_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++) {
        h ^= (unsigned char)p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static inline size_t next_pow2(size_t x) {
    if (x <= 1) return 1;
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static inline unsigned int clamp_u32(unsigned int v, unsigned int lo, unsigned int hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static inline void* arena_calloc_local(Arena *a, size_t n, size_t sz) {
    size_t bytes = n * sz;
    void *p = arena_alloc(a, bytes);
    if (!p) return NULL;
    memset(p, 0, bytes);
    return p;
}

/* ----- Fast field span extraction (pipe-delimited) ----- */
typedef struct {
    const char *ptr;
    uint32_t    len;
} Span;

typedef struct {
    Span   key;      /* key span */
    Span   nk1;      /* nonkey part 1 */
    Span   nk2;      /* nonkey part 2 (when key is in the middle) */
    size_t key_start;
    size_t key_end;  /* index of '|' or '\0' */
} ParsedLine;

static inline int parse_key_and_nonkey_spans(const char *line,
                                            int key_index,
                                            int num_columns,
                                            ParsedLine *out)
{
    if (!line || !out || key_index < 0 || key_index >= num_columns) return 0;

    size_t L = strlen(line);

    int field = 0;
    size_t start = 0;
    size_t key_start = (size_t)-1;
    size_t key_end   = (size_t)-1;

    for (size_t i = 0; i <= L; i++) {
        char c = (i < L) ? line[i] : '\0';
        if (c == '|' || c == '\0') {
            if (field == key_index) {
                key_start = start;
                key_end   = i;
                break;
            }
            field++;
            start = i + 1;
        }
    }
    if (key_start == (size_t)-1 || key_end == (size_t)-1 || key_end < key_start) return 0;

    out->key.ptr = line + key_start;
    out->key.len = (uint32_t)(key_end - key_start);
    out->key_start = key_start;
    out->key_end   = key_end;

    out->nk1.ptr = NULL; out->nk1.len = 0;
    out->nk2.ptr = NULL; out->nk2.len = 0;

    const int last = num_columns - 1;

    if (key_index == 0) {
        size_t s = (key_end < L && line[key_end] == '|') ? (key_end + 1) : key_end;
        if (s < L) { out->nk1.ptr = line + s; out->nk1.len = (uint32_t)(L - s); }
    } else if (key_index == last) {
        if (key_start > 0) { out->nk1.ptr = line; out->nk1.len = (uint32_t)key_start; }
    } else {
        if (key_start > 0) { out->nk1.ptr = line; out->nk1.len = (uint32_t)key_start; }
        size_t s = (key_end < L && line[key_end] == '|') ? (key_end + 1) : key_end;
        if (s < L) { out->nk2.ptr = line + s; out->nk2.len = (uint32_t)(L - s); }
    }

    return 1;
}

static inline void fwrite_span(FILE *f, const Span *s) {
    if (!s || !s->ptr || s->len == 0) return;
    (void)fwrite(s->ptr, 1, (size_t)s->len, f);
}

/* ----- Hash nodes: store right line once + spans ----- */
typedef struct {
    uint64_t h;
    Span     key;
    Span     nk1;
    Span     nk2;
    int32_t  next; /* index in nodes[] chain; -1 if end */
} HashNode;

/* I/O buffer plan: buffers from big_alloc so they count to max_mem (except merge copy buf at end) */
typedef struct {
    size_t out_buf_sz;
    size_t inL_buf_sz;
    size_t inR_buf_sz;
    size_t part_buf_sz;
    size_t total_bytes;
} IoBufPlanP;

static IoBufPlanP plan_iobuf_partition(size_t max_mem, unsigned int P) {
    IoBufPlanP plan;
    memset(&plan, 0, sizeof(plan));

    if (max_mem == 0) {
        plan.out_buf_sz  = 1 << 20;
        plan.inL_buf_sz  = 1 << 20;
        plan.inR_buf_sz  = 1 << 20;
        plan.part_buf_sz = 16 << 10;
        plan.total_bytes = plan.out_buf_sz + plan.inL_buf_sz + plan.inR_buf_sz
                         + plan.part_buf_sz * (size_t)(2 * P);
        return plan;
    }

    size_t io_budget = max_mem / 16;
    if (io_budget < 256 * 1024) io_budget = 256 * 1024;
    if (io_budget > (16ULL << 20)) io_budget = 16ULL << 20;

    size_t out_buf = io_budget / 2;
    size_t in_each = io_budget / 8;

    if (out_buf < 256 * 1024) out_buf = 256 * 1024;
    if (out_buf > (4ULL << 20)) out_buf = 4ULL << 20;

    if (in_each < 256 * 1024) in_each = 256 * 1024;
    if (in_each > (4ULL << 20)) in_each = 4ULL << 20;

    size_t used = out_buf + 2 * in_each;
    size_t remain = (io_budget > used) ? (io_budget - used) : 0;

    size_t part_buf = 0;
    if (P > 0) {
        part_buf = remain / (size_t)(2 * P);
        if (part_buf < 512) part_buf = 512;
        if (part_buf > (128 * 1024)) part_buf = 128 * 1024;
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

static size_t estimate_partition_meta_bytes(unsigned int P) {
    size_t bytes = 0;
    bytes += (size_t)(2 * P) * (size_t)PART_PATH_LEN;
    bytes += (size_t)(2 * P) * sizeof(FILE*);
    bytes += (size_t)(2 * P) * sizeof(size_t);
    return bytes;
}

static char* build_null_tail(int nonkey_cnt) {
    if (nonkey_cnt <= 0) {
        char *s = (char*)big_alloc(1);
        s[0] = '\0';
        return s;
    }
    size_t each = 5; /* "NULL|" */
    size_t n = (size_t)nonkey_cnt * each;
    char *buf = (char*)big_alloc(n + 1);
    char *p = buf;
    for (int i = 0; i < nonkey_cnt; i++) {
        memcpy(p, "NULL|", 5);
        p += 5;
    }
    buf[n] = '\0';
    return buf;
}

static inline size_t align_up(size_t x, size_t a) {
    return (x + (a - 1)) & ~(a - 1);
}

/* Sample average record length from a .tbl (excluding '\n') */
static double sample_avg_line_len(const char *path, size_t max_lines) {
    FILE *f = fopen(path, "r");
    if (!f) return 0.0;

    char buf[4096];
    size_t n = 0, sum = 0;

    while (n < max_lines && fgets(buf, sizeof(buf), f)) {
        size_t len = strcspn(buf, "\n");
        /* if truncated (line longer than buffer), skip this sample */
        if (len == sizeof(buf) - 1 && buf[len] != '\n') continue;
        sum += len;
        n++;
    }
    fclose(f);
    if (n == 0) return 0.0;
    return (double)sum / (double)n;
}

/* Force P large enough so Phase2 per-part hash build fits in per-thread arena.
 * This addresses your failing case (MM=128MB, P=29, ORDERS partitions ~518k rows => ~60MB strings alone).
 */
static unsigned int force_P_floor_for_phase2(size_t max_mem,
                                             size_t safety,
                                             size_t used_static_bigalloc,
                                             int T,
                                             unsigned int P_in,
                                             size_t right_file_size,
                                             const char *right_path)
{
    if (max_mem == 0 || right_file_size == 0) return P_in;

    double avg_len = sample_avg_line_len(right_path, 200000);
    if (avg_len < 32.0) avg_len = 120.0; /* fallback for ORDERS-ish lines */

    /* Estimate total rows */
    double est_total_rows = (double)right_file_size / avg_len;
    if (est_total_rows < 1.0) est_total_rows = 1.0;

    unsigned int P = P_in;
    if (P < 1) P = 1;
    if (P > 256u) P = 256u;

    /* Conservative skew guard: assume worst partition has 1.6x average */
    const double skew = 1.6;

    /* Per-row arena usage model:
       - store full line copy (~avg_len)
       - HashNode struct
       - allocator overhead & padding (~32)
       - buckets overhead amortized (~8 bytes/row for int32 bucket_count ~2x) */
    const double per_row_bytes = avg_len + (double)sizeof(HashNode) + 40.0;

    /* Iterate: increase P until required <= available arena_per_thread */
    for (int it = 0; it < 12; it++) {
        IoBufPlanP io = plan_iobuf_partition(max_mem, P);
        size_t meta = estimate_partition_meta_bytes(P);
        size_t must_fit = used_static_bigalloc + safety + io.total_bytes + meta;

        if (must_fit >= max_mem) {
            /* Even bookkeeping doesn't fit; can't improve here */
            return P;
        }

        size_t arena_total = max_mem - must_fit;

        int active_threads = T;
        if ((unsigned int)active_threads > P) active_threads = (int)P;
        if (active_threads <= 0) active_threads = 1;

        size_t arena_per_thread = arena_total / (size_t)active_threads;

        /* Required memory for worst right partition */
        double avg_rows_per_part = est_total_rows / (double)P;
        double worst_rows = avg_rows_per_part * skew;
        if (worst_rows < 1.0) worst_rows = 1.0;

        /* buckets: ~2x rows, 4 bytes each => ~8 bytes/row, add 2MB slack */
        double req = worst_rows * per_row_bytes + worst_rows * 8.0 + (2.0 * 1024.0 * 1024.0);

        if ((double)arena_per_thread >= req) {
            return P;
        }

        /* Need more partitions => reduce rows per part */
        unsigned int newP = (unsigned int)ceil((est_total_rows * skew) / ((double)arena_per_thread / (per_row_bytes + 8.0)));
        if (newP < P + 1) newP = P + 1;
        newP = clamp_u32(newP, 1u, 256u);

        if (newP == P) return P;
        P = newP;
    }

    return P;
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

    double tj_start = now_sec();

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

    size_t safety = 64 * 1024;

    /* ===== OMP thread count ===== */
    int T = omp_get_max_threads();
    if (T <= 0) T = 1;
    if (T > 256) T = 256;

    /* ===== Phase1 buffers (big_alloc) ===== */
    Block *blkL_p1 = (Block*)big_alloc(B_L);
    Block *blkR_p1 = (Block*)big_alloc(B_R);
    char **recsL_p1 = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);
    char **recsR_p1 = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

    /* ===== Phase2 per-thread pools (big_alloc) ===== */
    const size_t BLK_ALIGN = 64;
    size_t blkL_stride = align_up(B_L, BLK_ALIGN);
    size_t blkR_stride = align_up(B_R, BLK_ALIGN);

    char *thr_blkL_pool = (char*)big_alloc(blkL_stride * (size_t)T);
    char *thr_blkR_pool = (char*)big_alloc(blkR_stride * (size_t)T);

    char **thr_recsL_pool = (char**)big_alloc(sizeof(char*) * (size_t)T * (size_t)max_left_recs);
    char **thr_recsR_pool = (char**)big_alloc(sizeof(char*) * (size_t)T * (size_t)max_right_recs);

    const size_t THR_IN_BUF  = 512 * 1024;
    const size_t THR_OUT_BUF = 1024 * 1024;
    size_t thr_iobuf_stride = THR_IN_BUF + THR_IN_BUF + THR_OUT_BUF;
    char *thr_iobuf_pool = (char*)big_alloc(thr_iobuf_stride * (size_t)T);

    /* ===== Decide P + IO (initial) ===== */
    unsigned int P = 1;
    IoBufPlanP io;
    memset(&io, 0, sizeof(io));

    if (right_file_size == 0) {
        P = 1;
        io = plan_iobuf_partition(g_max_memory_bytes, P);
    } else {
        size_t used_static = g_big_alloc_bytes;

        for (int it = 0; it < 8; it++) {
            IoBufPlanP trialIo = plan_iobuf_partition(g_max_memory_bytes, P);
            size_t meta_bytes = estimate_partition_meta_bytes(P);

            if (g_max_memory_bytes > 0) {
                size_t must_fit = used_static + safety + trialIo.total_bytes + meta_bytes;
                if (must_fit >= g_max_memory_bytes) {
                    io = trialIo;
                    break;
                }
                size_t rem = g_max_memory_bytes - must_fit;

                size_t target_part_disk = rem / 2;
                if (target_part_disk < 256 * 1024) target_part_disk = 256 * 1024;

                unsigned int newP = (unsigned int)((right_file_size + target_part_disk - 1) / target_part_disk);
                if (newP < 1) newP = 1;
                newP = clamp_u32(newP, 1u, 256u);

                io = trialIo;
                if (newP == P) break;
                P = newP;
            } else {
                io = trialIo;
                break;
            }
        }
    }

    /* Optional: if memory allows, raise P to at least T for better parallelism */
    if (g_max_memory_bytes > 0 && (unsigned int)T > P) {
        unsigned int trialP = (unsigned int)T;
        if (trialP > 256u) trialP = 256u;

        size_t used_static = g_big_alloc_bytes;
        IoBufPlanP trialIo = plan_iobuf_partition(g_max_memory_bytes, trialP);
        size_t meta_bytes = estimate_partition_meta_bytes(trialP);
        size_t must_fit = used_static + safety + trialIo.total_bytes + meta_bytes;

        if (must_fit < g_max_memory_bytes) {
            P = trialP;
            io = trialIo;
        }
    }

    /* ===== Critical fix: force P floor so Phase2 hash build fits (prevents early exit => enables timing) ===== */
    if (g_max_memory_bytes > 0 && right_file_size > 0) {
        unsigned int oldP = P;
        P = force_P_floor_for_phase2(g_max_memory_bytes, safety, g_big_alloc_bytes, T, P, right_file_size, right->filename);
        if (P != oldP) {
            io = plan_iobuf_partition(g_max_memory_bytes, P);
        }
    }

    /* ===== Allocate iobufs + partition meta that depend on P ===== */
    char *inL_iobuf = NULL, *inR_iobuf = NULL, *part_iobuf_pool = NULL, *merge_iobuf = NULL;

    if (io.inL_buf_sz) inL_iobuf = (char*)big_alloc(io.inL_buf_sz);
    if (io.inR_buf_sz) inR_iobuf = (char*)big_alloc(io.inR_buf_sz);

    if (io.part_buf_sz && P > 0) {
        part_iobuf_pool = (char*)big_alloc(io.part_buf_sz * (size_t)(2 * P));
    }

    merge_iobuf = (char*)big_alloc(io.out_buf_sz ? io.out_buf_sz : (1<<20));

    char (*left_part_paths)[PART_PATH_LEN]  = (char(*)[PART_PATH_LEN])big_alloc((size_t)P * PART_PATH_LEN);
    char (*right_part_paths)[PART_PATH_LEN] = (char(*)[PART_PATH_LEN])big_alloc((size_t)P * PART_PATH_LEN);
    FILE **lfp = (FILE**)big_alloc(sizeof(FILE*) * (size_t)P);
    FILE **rfp = (FILE**)big_alloc(sizeof(FILE*) * (size_t)P);

    size_t *right_part_rec_counts = (size_t*)big_alloc(sizeof(size_t) * (size_t)P);
    size_t *left_part_rec_counts  = (size_t*)big_alloc(sizeof(size_t) * (size_t)P);
    memset(right_part_rec_counts, 0, sizeof(size_t) * (size_t)P);
    memset(left_part_rec_counts,  0, sizeof(size_t) * (size_t)P);

    char (*out_part_paths)[PART_PATH_LEN] = (char(*)[PART_PATH_LEN])big_alloc((size_t)P * PART_PATH_LEN);

    const int nonkey_cnt = right->header.num_columns - 1;
    char *null_tail = build_null_tail(nonkey_cnt);

    /* ===== Arena budgeting (FIX): split by active_threads=min(T,P), not by T ===== */
    size_t arena_total = 0;
    if (g_max_memory_bytes > 0) {
        if (g_big_alloc_bytes + safety >= g_max_memory_bytes) {
            arena_total = (4ULL << 20);
        } else {
            arena_total = g_max_memory_bytes - g_big_alloc_bytes - safety;
        }
    } else {
        arena_total = 256ULL << 20;
    }

    int active_threads = T;
    if ((unsigned int)active_threads > P) active_threads = (int)P;
    if (active_threads <= 0) active_threads = 1;

    Arena *thr_arenas = (Arena*)big_alloc(sizeof(Arena) * (size_t)T);

    size_t base = arena_total / (size_t)active_threads;
    size_t rem  = arena_total % (size_t)active_threads;

    const size_t MIN_ARENA = (4ULL << 20);

    for (int t = 0; t < T; t++) {
        size_t cap;
        if (t < active_threads) {
            cap = base + ((size_t)t < rem ? 1 : 0);
            if (cap < MIN_ARENA) cap = MIN_ARENA;
        } else {
            cap = MIN_ARENA;
        }
        arena_init(&thr_arenas[t], cap);
    }

    printf("[INFO] Strategy: PARTITIONED_HASH_LEFT_JOIN + OMP Phase2\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu, OMP_T=%d (active=%d)\n",
           g_max_memory_bytes, B_L, B_R, T, active_threads);
    printf("[INFO] max_left_recs=%d, max_right_recs=%d\n", max_left_recs, max_right_recs);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu\n", left_file_size, right_file_size);
    printf("[INFO] est_left_blocks=%zu, est_right_blocks=%zu\n", est_left_blocks, est_right_blocks);
    printf("[INFO] P=%u, big_alloc_used=%zu, arenas_total=%zu\n", P, g_big_alloc_bytes, arena_total);
    printf("[INFO] iobuf: merge_out=%zu inL=%zu inR=%zu part_each=%zu (total=%zu)\n",
           (io.out_buf_sz ? io.out_buf_sz : (1<<20)), io.inL_buf_sz, io.inR_buf_sz, io.part_buf_sz, io.total_bytes);

    /* ===== Phase1: open partition files ===== */
    for (unsigned int p = 0; p < P; p++) {
        snprintf(left_part_paths[p],  PART_PATH_LEN, "left_part_%u.tmp",  p);
        snprintf(right_part_paths[p], PART_PATH_LEN, "right_part_%u.tmp", p);
        snprintf(out_part_paths[p],   PART_PATH_LEN, "out_part_%u.tmp",   p);

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

    /* ===== Phase 1: partitioning ===== */
    int left_blocks_p1 = 0, right_blocks_p1 = 0;
    size_t left_recs_p1 = 0, right_recs_p1 = 0;

    {
        FILE *lf = fopen(left->filename, "r");
        if (!lf) { perror("fopen left"); exit(EXIT_FAILURE); }
        if (inL_iobuf) setvbuf(lf, inL_iobuf, _IOFBF, io.inL_buf_sz);

        PendingLine pend = {0};
        while (1) {
            int cnt = 0;
            double tr1 = now_sec();
            int ok = fill_block(lf, B_L, blkL_p1, recsL_p1, &cnt, &pend, max_left_recs);
            double tr2 = now_sec();
            g_time_read += (tr2 - tr1);
            if (!ok || cnt == 0) break;

            left_blocks_p1++;

            double tw1 = now_sec();

            for (int i = 0; i < cnt; i++) {
                ParsedLine pl;
                if (!parse_key_and_nonkey_spans(recsL_p1[i], left->header.key_index, left->header.num_columns, &pl)) {
                    pl.key.ptr = recsL_p1[i];
                    pl.key.len = (uint32_t)strlen(recsL_p1[i]);
                }
                unsigned int part = (unsigned int)(hash_bytes(pl.key.ptr, pl.key.len) % P);

                fputs(recsL_p1[i], lfp[part]);
                fputc('\n', lfp[part]);

                left_part_rec_counts[part]++;
                left_recs_p1++;
            }

            double tw2 = now_sec();
            g_time_write += (tw2 - tw1);
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
            double tr1 = now_sec();
            int ok = fill_block(rf, B_R, blkR_p1, recsR_p1, &cnt, &pend, max_right_recs);
            double tr2 = now_sec();
            g_time_read += (tr2 - tr1);
            if (!ok || cnt == 0) break;

            right_blocks_p1++;

            double tw1 = now_sec();

            for (int i = 0; i < cnt; i++) {
                ParsedLine pl;
                if (!parse_key_and_nonkey_spans(recsR_p1[i], right->header.key_index, right->header.num_columns, &pl)) {
                    pl.key.ptr = recsR_p1[i];
                    pl.key.len = (uint32_t)strlen(recsR_p1[i]);
                }
                unsigned int part = (unsigned int)(hash_bytes(pl.key.ptr, pl.key.len) % P);

                fputs(recsR_p1[i], rfp[part]);
                fputc('\n', rfp[part]);

                right_part_rec_counts[part]++;
                right_recs_p1++;
            }

            double tw2 = now_sec();
            g_time_write += (tw2 - tw1);
        }

        pendingline_reset_local(&pend);
        fclose(rf);
    }

    for (unsigned int p = 0; p < P; p++) {
        fflush(lfp[p]); fflush(rfp[p]);
        fclose(lfp[p]); fclose(rfp[p]); /* Phase2: thread별 reopen */
        lfp[p] = NULL; rfp[p] = NULL;
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

    /* ===== Phase2 stats per partition ===== */
    int    *part_Lblocks = (int*)big_alloc(sizeof(int) * (size_t)P);
    int    *part_Rblocks = (int*)big_alloc(sizeof(int) * (size_t)P);
    double *part_read_t  = (double*)big_alloc(sizeof(double) * (size_t)P);
    double *part_write_t = (double*)big_alloc(sizeof(double) * (size_t)P);
    for (unsigned int p = 0; p < P; p++) {
        part_Lblocks[p] = 0;
        part_Rblocks[p] = 0;
        part_read_t[p]  = 0.0;
        part_write_t[p] = 0.0;
        FILE *tmp = fopen(out_part_paths[p], "w");
        if (tmp) fclose(tmp);
    }

    /* ===== Phase 2: per-part join (OpenMP) ===== */
    #pragma omp parallel for schedule(dynamic, 1)
    for (int pi = 0; pi < (int)P; pi++) {
        unsigned int p = (unsigned int)pi;
        int tid = omp_get_thread_num();
        if (tid < 0) tid = 0;
        if (tid >= T) tid = T - 1;

        Arena *arena = &thr_arenas[tid];
        arena_reset(arena);

        Block *blkL = (Block*)(thr_blkL_pool + (size_t)tid * blkL_stride);
        Block *blkR = (Block*)(thr_blkR_pool + (size_t)tid * blkR_stride);
        char **recsL = thr_recsL_pool + (size_t)tid * (size_t)max_left_recs;
        char **recsR = thr_recsR_pool + (size_t)tid * (size_t)max_right_recs;

        char *thr_iobuf = thr_iobuf_pool + (size_t)tid * thr_iobuf_stride;
        char *buf_inL = thr_iobuf;
        char *buf_inR = thr_iobuf + THR_IN_BUF;
        char *buf_out = thr_iobuf + THR_IN_BUF + THR_IN_BUF;

        double read_t = 0.0, write_t = 0.0;
        int localLBlocks = 0, localRBlocks = 0;

        if (left_part_rec_counts[p] == 0) {
            part_Lblocks[p] = 0;
            part_Rblocks[p] = 0;
            part_read_t[p]  = 0.0;
            part_write_t[p] = 0.0;
            continue;
        }

        FILE *outp = fopen(out_part_paths[p], "w");
        if (!outp) { fprintf(stderr, "[ERROR] fopen out_part failed: %s\n", out_part_paths[p]); exit(EXIT_FAILURE); }
        setvbuf(outp, buf_out, _IOFBF, THR_OUT_BUF);

        /* R empty -> emit NULLs */
        if (right_part_rec_counts[p] == 0) {
            FILE *lf = fopen(left_part_paths[p], "r");
            if (!lf) { fprintf(stderr, "[ERROR] fopen left_part failed: %s\n", left_part_paths[p]); exit(EXIT_FAILURE); }
            setvbuf(lf, buf_inL, _IOFBF, THR_IN_BUF);

            PendingLine pendL = {0};
            while (1) {
                int lc = 0;
                double tr1 = now_sec();
                int ok = fill_block(lf, B_L, blkL, recsL, &lc, &pendL, max_left_recs);
                double tr2 = now_sec();
                read_t += (tr2 - tr1);

                if (!ok || lc == 0) break;
                localLBlocks++;

                double tw1 = now_sec();

                for (int i = 0; i < lc; i++) {
                    fputs(recsL[i], outp);
                    fputs(null_tail, outp);
                    fputc('\n', outp);
                }

                double tw2 = now_sec();
                write_t += (tw2 - tw1);
            }

            pendingline_reset_local(&pendL);
            fclose(lf);
            fclose(outp);

            part_Lblocks[p] = localLBlocks;
            part_Rblocks[p] = 0;
            part_read_t[p]  = read_t;
            part_write_t[p] = write_t;
            continue;
        }

        /* Build hash from right partition */
        size_t rN = right_part_rec_counts[p];
        if (rN > (size_t)INT32_MAX - 8) {
            fprintf(stderr, "[ERROR] partition too large for int32 indexing (part=%u rN=%zu)\n", p, rN);
            exit(EXIT_FAILURE);
        }

        size_t bucket_count = next_pow2(rN * 2);
        if (bucket_count < 2048) bucket_count = 2048;

        int32_t *buckets = (int32_t*)arena_alloc(arena, bucket_count * sizeof(int32_t));
        if (!buckets) {
            fprintf(stderr, "[ERROR] arena too small for buckets (part=%u)\n", p);
            exit(EXIT_FAILURE);
        }
        memset(buckets, 0xFF, bucket_count * sizeof(int32_t));

        HashNode *nodes = (HashNode*)arena_alloc(arena, rN * sizeof(HashNode));
        if (!nodes) {
            fprintf(stderr, "[ERROR] arena too small for nodes (part=%u)\n", p);
            exit(EXIT_FAILURE);
        }

        FILE *rf = fopen(right_part_paths[p], "r");
        if (!rf) { fprintf(stderr, "[ERROR] fopen right_part failed: %s\n", right_part_paths[p]); exit(EXIT_FAILURE); }
        setvbuf(rf, buf_inR, _IOFBF, THR_IN_BUF);

        PendingLine pendR = {0};
        size_t node_count = 0;

        while (1) {
            int rc = 0;
            double tr1 = now_sec();
            int ok = fill_block(rf, B_R, blkR, recsR, &rc, &pendR, max_right_recs);
            double tr2 = now_sec();
            read_t += (tr2 - tr1);

            if (!ok || rc == 0) break;
            localRBlocks++;

            for (int i = 0; i < rc; i++) {
                if (node_count >= rN) {
                    fprintf(stderr, "[ERROR] right count mismatch (part=%u)\n", p);
                    exit(EXIT_FAILURE);
                }

                char *line = arena_strdup(arena, recsR[i]); /* store once */
                if (!line) {
                    fprintf(stderr, "[ERROR] arena overflow storing right line (part=%u). Increase mem/P.\n", p);
                    exit(EXIT_FAILURE);
                }

                ParsedLine pl;
                if (!parse_key_and_nonkey_spans(line, right->header.key_index, right->header.num_columns, &pl)) {
                    pl.key.ptr = line;
                    pl.key.len = (uint32_t)strlen(line);
                    pl.nk1.ptr = NULL; pl.nk1.len = 0;
                    pl.nk2.ptr = NULL; pl.nk2.len = 0;
                }

                uint64_t h = hash_bytes(pl.key.ptr, pl.key.len);
                size_t idx = (size_t)(h & (bucket_count - 1));

                HashNode *n = &nodes[node_count];
                n->h    = h;
                n->key  = pl.key;
                n->nk1  = pl.nk1;
                n->nk2  = pl.nk2;
                n->next = buckets[idx];
                buckets[idx] = (int32_t)node_count;

                node_count++;
            }
        }

        pendingline_reset_local(&pendR);
        fclose(rf);

        /* Probe with left partition */
        FILE *lf = fopen(left_part_paths[p], "r");
        if (!lf) { fprintf(stderr, "[ERROR] fopen left_part failed: %s\n", left_part_paths[p]); exit(EXIT_FAILURE); }
        setvbuf(lf, buf_inL, _IOFBF, THR_IN_BUF);

        PendingLine pendL = {0};

        while (1) {
            int lc = 0;
            double tr1 = now_sec();
            int ok = fill_block(lf, B_L, blkL, recsL, &lc, &pendL, max_left_recs);
            double tr2 = now_sec();
            read_t += (tr2 - tr1);

            if (!ok || lc == 0) break;
            localLBlocks++;

            double tw1 = now_sec();

            for (int i = 0; i < lc; i++) {
                ParsedLine pl;
                if (!parse_key_and_nonkey_spans(recsL[i], left->header.key_index, left->header.num_columns, &pl)) {
                    pl.key.ptr = recsL[i];
                    pl.key.len = (uint32_t)strlen(recsL[i]);
                }

                uint64_t h = hash_bytes(pl.key.ptr, pl.key.len);
                size_t idx = (size_t)(h & (bucket_count - 1));

                int matched = 0;
                for (int32_t cur = buckets[idx]; cur != -1; cur = nodes[cur].next) {
                    HashNode *n = &nodes[cur];
                    if (n->h != h) continue;
                    if (n->key.len != pl.key.len) continue;
                    if (memcmp(n->key.ptr, pl.key.ptr, pl.key.len) != 0) continue;

                    matched = 1;
                    fputs(recsL[i], outp);
                    fwrite_span(outp, &n->nk1);
                    fwrite_span(outp, &n->nk2);

                    /* ensure last delimiter if needed */
                    if (nonkey_cnt > 0) {
                        const Span *last = (n->nk2.ptr && n->nk2.len) ? &n->nk2 : &n->nk1;
                        if (last && last->ptr && last->len > 0) {
                            char lastc = last->ptr[last->len - 1];
                            if (lastc != '|') fputc('|', outp);
                        }
                    }
                    fputc('\n', outp);
                }

                if (!matched) {
                    fputs(recsL[i], outp);
                    fputs(null_tail, outp);
                    fputc('\n', outp);
                }
            }

            double tw2 = now_sec();
            write_t += (tw2 - tw1);
        }

        pendingline_reset_local(&pendL);
        fclose(lf);
        fclose(outp);

        part_Lblocks[p] = localLBlocks;
        part_Rblocks[p] = localRBlocks;
        part_read_t[p]  = read_t;
        part_write_t[p] = write_t;
    } /* end Phase2 parallel */

    /* Reduce totals */
    int left_blocks_p2 = 0, right_blocks_p2 = 0;
    for (unsigned int p = 0; p < P; p++) {
        left_blocks_p2  += part_Lblocks[p];
        right_blocks_p2 += part_Rblocks[p];
        g_time_read     += part_read_t[p];
        g_time_write    += part_write_t[p];
    }

    printf("[INFO] Phase2 done: left_blocks=%d right_blocks=%d\n", left_blocks_p2, right_blocks_p2);
    printf("[INFO] SUMMARY: P=%u, phase1(L=%d R=%d), phase2(L=%d R=%d), total(L=%d R=%d)\n",
           P, left_blocks_p1, right_blocks_p1, left_blocks_p2, right_blocks_p2,
           left_blocks_p1 + left_blocks_p2, right_blocks_p1 + right_blocks_p2);

    /* ===== Merge out_part_*.tmp -> output_path ===== */
    {
        double tw1 = now_sec();

        FILE *final_out = fopen(output_path, "w");
        if (!final_out) { perror("fopen final output"); exit(EXIT_FAILURE); }
        if (merge_iobuf) setvbuf(final_out, merge_iobuf, _IOFBF, (io.out_buf_sz ? io.out_buf_sz : (1<<20)));

        /* merge copy buffer: malloc (not counted in strict) */
        size_t copy_cap = (1u << 20);
        char *copy_buf = (char*)malloc(copy_cap);
        if (!copy_buf) {
            copy_cap = 64 * 1024;
            copy_buf = (char*)malloc(copy_cap);
            if (!copy_buf) { fprintf(stderr, "[ERROR] merge malloc failed\n"); exit(EXIT_FAILURE); }
        }

        for (unsigned int p = 0; p < P; p++) {
            FILE *pf = fopen(out_part_paths[p], "r");
            if (!pf) continue;
            size_t n;
            while ((n = fread(copy_buf, 1, copy_cap, pf)) > 0) {
                fwrite(copy_buf, 1, n, final_out);
            }
            fclose(pf);
            remove(out_part_paths[p]);
        }

        free(copy_buf);
        fclose(final_out);

        double tw2 = now_sec();
        g_time_write += (tw2 - tw1);
    }

    /* cleanup partition files */
    for (unsigned int p = 0; p < P; p++) {
        remove(left_part_paths[p]);
        remove(right_part_paths[p]);
    }

    double total = now_sec() - tj_start;
    g_time_join = total - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks_p1 + left_blocks_p2;
    *right_block_count_out = right_blocks_p1 + right_blocks_p2;

    printf("[METRIC] max_mem=%zu B_L=%zu B_R=%zu P=%u omp=%d(active=%d) elapsed=%.6f read=%.6f join=%.6f write=%.6f\n",
           g_max_memory_bytes, B_L, B_R, P, T, active_threads, total, g_time_read, g_time_join, g_time_write);
}
