// join_with_mem_limit.c (DEBUG 버전)
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include "time_utils.h"
#include "table_split.h"

typedef struct {
    unsigned char *page;  // 한 페이지(=page_size) 버퍼
} RFrame;

typedef struct {
    unsigned char *page;  // 한 페이지(=page_size) 버퍼
} SFrame;

/* 조인 결과 한 줄 저장용 */
typedef struct {
    char *line;           // "R레코드|S레코드\n\0" 형태
} JoinedRow;

/* 에러 처리 + 자원 해제 후 -1 반환 (결과 버퍼는 여기서는 해제 안 함) */
static int finalize_error(
    const char *msg,
    RFrame *r_frames, size_t framesR,
    SFrame *s_frames, size_t framesS,
    int fdR, int fdS
) {
    if (msg) perror(msg);

    if (r_frames) {
        for (size_t i = 0; i < framesR; ++i) {
            if (r_frames[i].page) free(r_frames[i].page);
        }
        free(r_frames);
    }
    if (s_frames) {
        for (size_t j = 0; j < framesS; ++j) {
            if (s_frames[j].page) free(s_frames[j].page);
        }
        free(s_frames);
    }
    if (fdR >= 0) close(fdR);
    if (fdS >= 0) close(fdS);

    return -1;
}

/* "조인된 한 행"을 결과 배열에 추가 */
static int append_joined_row(
    JoinedRow **rows,
    size_t *count,
    size_t *cap,
    const char *r_rec, size_t r_len,
    const char *s_rec, size_t s_len
) {
    /* 1. R 레코드 끝의 '|' 제거 (있다면) */
    size_t r_eff = r_len;
    while (r_eff > 0 && (r_rec[r_eff - 1] == ' ' || r_rec[r_eff - 1] == '\t'))
        r_eff--;  // 뒤 공백 제거

    if (r_eff > 0 && r_rec[r_eff - 1] == '|') {
        r_eff--;  // 마지막 '|' 제거
    }

    /* 2. S 레코드 끝의 개행/CR 만 제거 (마지막 '|'는 그대로 둠) */
    size_t s_eff = s_len;
    while (s_eff > 0 &&
           (s_rec[s_eff - 1] == '\n' || s_rec[s_eff - 1] == '\r'))
        s_eff--;

    /* 3. R + '|' + S + '\n' + '\0' 용 버퍼 확보 */
    size_t total = r_eff + 1 + s_eff;          // R + '|' + S
    char *buf = (char *)malloc(total + 2);     // + '\n' + '\0'
    if (!buf) return -1;

    /* 4. 복사 */
    memcpy(buf, r_rec, r_eff);                 // R
    buf[r_eff] = '|';                          // 구분자
    memcpy(buf + r_eff + 1, s_rec, s_eff);     // S

    buf[total]     = '\n';                     // 줄바꿈
    buf[total + 1] = '\0';                     // 문자열 종료

    /* 5. JoinedRow 배열 확장 */
    if (*count == *cap) {
        size_t new_cap = (*cap == 0) ? 1024 : (*cap * 2);
        JoinedRow *tmp = (JoinedRow *)realloc(*rows, new_cap * sizeof(JoinedRow));
        if (!tmp) {
            free(buf);
            return -1;
        }
        *rows = tmp;
        *cap  = new_cap;
    }

    (*rows)[*count].line = buf;
    (*count)++;
    return 0;
}


/* 레코드(한 줄)에서 "첫 번째 필드(맨 앞 숫자)"를 long으로 파싱 (PARTKEY 용) */
static int extract_first_key_long(const char *rec, size_t len, long *out_key) {
    if (len == 0) return 0;

    size_t i = 0;
    /* 첫 번째 필드 끝(또는 '|' 위치)까지 */
    while (i < len && rec[i] != '|') {
        i++;
    }
    size_t key_len = i;          // rec[0..key_len-1] 가 첫 필드
    if (key_len == 0) return 0;  // 비어 있으면 실패

    const char *start = rec;

    /* 앞/뒤 공백 제거 */
    while (key_len > 0 &&
           (start[key_len - 1] == ' ' || start[key_len - 1] == '\t'))
        key_len--;
    while (key_len > 0 &&
           (*start == ' ' || *start == '\t')) {
        start++;
        key_len--;
    }

    if (key_len == 0) return 0;

    char buf[64];
    if (key_len >= sizeof(buf)) return 0;

    memcpy(buf, start, key_len);
    buf[key_len] = '\0';

    char *endp = NULL;
    long v = strtol(buf, &endp, 10);
    if (endp == buf) return 0;   // 숫자 파싱 실패

    *out_key = v;
    return 1;
}

/*
 * 페이지 하나에서 레코드를 순회하면서,
 *   - 각 레코드의 PARTKEY를 추출하고
 *   - 콜백 함수 cb(rec_ptr, rec_len, key, user_data)를 호출
 *
 * 페이지 포맷 가정:
 *   - page[0..page_size-1] 영역에서
 *   - 유효 데이터는 '\n'으로 끝나는 텍스트 레코드들의 연속
 *   - 남는 영역은 '\0' 패딩
 */
typedef void (*record_callback)(
    const char *rec, size_t len, long key, void *user_data
);

static void parse_page_records(
    unsigned char *page,
    size_t page_size,
    record_callback cb,
    void *user_data
) {
    size_t pos = 0;
    while (pos < page_size) {
        if (page[pos] == '\0') break;  // 패딩 영역 → 종료

        char *rec = (char *)(page + pos);
        size_t maxlen = page_size - pos;

        char *nl = memchr(rec, '\n', maxlen);
        size_t len;
        if (nl) {
            len = (size_t)(nl - rec);
        } else {
            len = maxlen;
        }

        if (len == 0) {
            if (nl) pos = (size_t)(nl - (char *)page) + 1;
            else break;
            continue;
        }

        size_t eff_len = len;
        if (eff_len > 0 && rec[eff_len - 1] == '\r')
            eff_len--;

        long key;
        if (extract_first_key_long(rec, eff_len, &key)) {
            cb(rec, eff_len, key, user_data);
        }

        if (nl)
            pos = (size_t)(nl - (char *)page) + 1;
        else
            break;
    }
}

/*
 * r_file      : PART 쪽 paged 파일 경로 (예: "part.tbl_4096.dat")
 * pageR       : PART 페이지 크기
 * s_file      : PARTSUPP 쪽 paged 파일 경로 (예: "partsupp.tbl_4096.dat")
 * pageS       : PARTSUPP 페이지 크기
 * mem_limit   : 조인에 쓸 버퍼 메모리 제한 (R, S 페이지 버퍼만 해당)
 *
 * JOIN 조건   : PART.PARTKEY = PARTSUPP.PARTKEY  (각 레코드의 첫 번째 숫자 필드)
 * 결과        : 조인될 때마다 "PART레코드|PARTSUPP레코드" 를 별도 메모리에 저장 후
 *               마지막에 전부 출력
 */
int run_join_with_mem_limit(
    const char *r_file, size_t pageR,
    const char *s_file, size_t pageS,
    size_t mem_limit,
    const char *out_file
) {
    int fdR = -1, fdS = -1;
    RFrame *r_frames = NULL;
    SFrame *s_frames = NULL;
    size_t framesR = 0, framesS = 0;

    JoinedRow *rows = NULL;          // 조인 결과
    size_t rows_count = 0;
    size_t rows_cap   = 0;

    /* 1. 파일 열기 */
    fdR = open(r_file, O_RDONLY);
    if (fdR < 0) {
        return finalize_error("open R", r_frames, framesR, s_frames, framesS, fdR, fdS);
    }
    fdS = open(s_file, O_RDONLY);
    if (fdS < 0) {
        return finalize_error("open S", r_frames, framesR, s_frames, framesS, fdR, fdS);
    }

    /* 2. 파일 크기 → 블록(페이지) 개수 */
    struct stat stR, stS;
    if (fstat(fdR, &stR) < 0) {
        return finalize_error("fstat R", r_frames, framesR, s_frames, framesS, fdR, fdS);
    }
    if (fstat(fdS, &stS) < 0) {
        return finalize_error("fstat S", r_frames, framesR, s_frames, framesS, fdR, fdS);
    }

    if (stR.st_size % (off_t)pageR != 0) {
        fprintf(stderr, "R file size (%lld) is not multiple of pageR (%zu)\n",
                (long long)stR.st_size, pageR);
        return finalize_error(NULL, r_frames, framesR, s_frames, framesS, fdR, fdS);
    }
    if (stS.st_size % (off_t)pageS != 0) {
        fprintf(stderr, "S file size (%lld) is not multiple of pageS (%zu)\n",
                (long long)stS.st_size, pageS);
        return finalize_error(NULL, r_frames, framesR, s_frames, framesS, fdR, fdS);
    }

    size_t blocksR = (size_t)stR.st_size / pageR;
    size_t blocksS = (size_t)stS.st_size / pageS;

    printf("R blocks: %zu (page %zu)\n", blocksR, pageR);
    printf("S blocks: %zu (page %zu)\n", blocksS, pageS);

    /* 3. 제한 메모리 안에서 프레임 수 결정 (R/S 반반) */
    size_t half = mem_limit / 2;
    framesR = half / pageR;
    framesS = half / pageS;
    if (framesR < 1) framesR = 1;
    if (framesS < 1) framesS = 1;

    size_t used_mem = framesR * pageR + framesS * pageS;
    printf("Memory limit : %zu bytes\n", mem_limit);
    printf("Frames (R)   : %zu (each %zu bytes)\n", framesR, pageR);
    printf("Frames (S)   : %zu (each %zu bytes)\n", framesS, pageS);
    printf("Used buffer  : %zu bytes\n\n", used_mem);

    /* 진행률 계산용 */
    uint64_t total_pairs = (uint64_t)blocksR * (uint64_t)blocksS;
    uint64_t done_pairs  = 0;
    int      last_percent = -1;

    /* [DEBUG] 기본 정보 한 번 출력 */
    fprintf(stderr,
            "[DEBUG] Join start: blocksR=%zu, blocksS=%zu, "
            "framesR=%zu, framesS=%zu, total_pairs=%llu\n",
            blocksR, blocksS,
            framesR, framesS,
            (unsigned long long)total_pairs);
    fflush(stderr);

    /* 4. 프레임 배열 할당 */
    r_frames = (RFrame *)calloc(framesR, sizeof(RFrame));
    s_frames = (SFrame *)calloc(framesS, sizeof(SFrame));
    if (!r_frames || !s_frames) {
        return finalize_error("calloc frames",
                              r_frames, framesR, s_frames, framesS, fdR, fdS);
    }

    for (size_t i = 0; i < framesR; ++i) {
        r_frames[i].page = (unsigned char *)malloc(pageR);
        if (!r_frames[i].page) {
            return finalize_error("malloc r_frames[i].page",
                                  r_frames, framesR, s_frames, framesS, fdR, fdS);
        }
    }
    for (size_t j = 0; j < framesS; ++j) {
        s_frames[j].page = (unsigned char *)malloc(pageS);
        if (!s_frames[j].page) {
            return finalize_error("malloc s_frames[j].page",
                                  r_frames, framesR, s_frames, framesS, fdR, fdS);
        }
    }

    /* 5. 성능 측정 시작 */
    double t0 = now_ns();
    uint64_t r_page_reads = 0;
    uint64_t s_page_reads = 0;
    uint64_t join_count   = 0;

    /* 콜백에서 사용할 컨텍스트 구조들 */

    /* S 페이지 한 개를 스캔할 때 사용 */
    typedef struct {
        const char *r_rec;
        size_t      r_len;
        long        r_key;
        uint64_t   *p_join_count;
        JoinedRow **p_rows;
        size_t     *p_rows_count;
        size_t     *p_rows_cap;
    } SScanCtx;

    void s_record_cb(const char *rec, size_t len, long s_key, void *user_data) {
        SScanCtx *ctx = (SScanCtx *)user_data;

        /* [DEBUG] S 레코드 진행 상황 */
        static uint64_t s_rec_counter = 0;
        s_rec_counter++;
        if ((s_rec_counter % 500000) == 0) {  // 50만 개마다 한 번
            fprintf(stderr,
                    "[DEBUG]   S records processed: %llu "
                    "(current r_key=%ld, s_key=%ld, join_count=%llu)\n",
                    (unsigned long long)s_rec_counter,
                    ctx->r_key,
                    s_key,
                    (unsigned long long)(*(ctx->p_join_count)));
            fflush(stderr);
        }

        if (s_key == ctx->r_key) {
            (*(ctx->p_join_count))++;
            /* 조인된 한 행을 결과 버퍼에 저장 (PART행 | PARTSUPP행) */
            if (append_joined_row(ctx->p_rows,
                                  ctx->p_rows_count,
                                  ctx->p_rows_cap,
                                  ctx->r_rec, ctx->r_len,
                                  rec, len) != 0) {
                // 메모리 부족 시 여기서는 단순히 넘어감
            }
        }
    }

    /* R 페이지 한 개를 스캔할 때 사용 */
    typedef struct {
        SFrame   *s_frames;
        size_t    s_loaded;
        size_t    pageS;
        uint64_t *p_join_count;
        JoinedRow **p_rows;
        size_t     *p_rows_count;
        size_t     *p_rows_cap;
    } RScanCtx;

    void r_record_cb(const char *rec, size_t len, long r_key, void *user_data) {
        RScanCtx *ctx = (RScanCtx *)user_data;

        /* [DEBUG] R 레코드 진행 상황 */
        static uint64_t r_rec_counter = 0;
        r_rec_counter++;
        if ((r_rec_counter % 100000) == 0) {  // 10만 개마다 한 번
            fprintf(stderr,
                    "[DEBUG] R records processed: %llu, "
                    "current r_key=%ld, join_count=%llu\n",
                    (unsigned long long)r_rec_counter,
                    r_key,
                    (unsigned long long)(*(ctx->p_join_count)));
            fflush(stderr);
        }

        /* 현재 R 레코드 하나에 대해, 로드된 모든 S 페이지를 탐색 */
        for (size_t sj = 0; sj < ctx->s_loaded; ++sj) {
            unsigned char *s_page = ctx->s_frames[sj].page;

            Block *s_blk = (Block *)s_page;
            unsigned char *s_data = s_blk->data;
            size_t s_data_sz = ctx->pageS - sizeof(BlockHeader);

            SScanCtx sctx;
            sctx.r_rec        = rec;
            sctx.r_len        = len;
            sctx.r_key        = r_key;
            sctx.p_join_count = ctx->p_join_count;
            sctx.p_rows       = ctx->p_rows;
            sctx.p_rows_count = ctx->p_rows_count;
            sctx.p_rows_cap   = ctx->p_rows_cap;

            parse_page_records(s_data, s_data_sz, s_record_cb, &sctx);
        }
    }

    /* 6. 블록 Nested-Loop Join */
    for (size_t r_start = 0; r_start < blocksR; r_start += framesR) {

        fprintf(stderr,
                "[DEBUG] R chunk start: r_start=%zu (blocksR=%zu)\n",
                r_start, blocksR);
        fflush(stderr);

        /* R 페이지 묶음 로딩 */
        size_t r_loaded = 0;
        for (size_t ri = 0; ri < framesR && (r_start + ri) < blocksR; ++ri) {
            off_t off = (off_t)(r_start + ri) * (off_t)pageR;
            ssize_t n = pread(fdR, r_frames[ri].page, pageR, off);
            if (n != (ssize_t)pageR) {
                return finalize_error("pread R", r_frames, framesR, s_frames, framesS, fdR, fdS);
            }
            r_loaded++;
            r_page_reads++;
        }

        for (size_t s_start = 0; s_start < blocksS; s_start += framesS) {

            fprintf(stderr,
                    "[DEBUG]   S chunk start: s_start=%zu (blocksS=%zu)\n",
                    s_start, blocksS);
            fflush(stderr);

            /* S 페이지 묶음 로딩 */
            size_t s_loaded = 0;
            for (size_t sj = 0; sj < framesS && (s_start + sj) < blocksS; ++sj) {
                off_t off = (off_t)(s_start + sj) * (off_t)pageS;
                ssize_t n = pread(fdS, s_frames[sj].page, pageS, off);
                if (n != (ssize_t)pageS) {
                    return finalize_error("pread S", r_frames, framesR, s_frames, framesS, fdR, fdS);
                }
                s_loaded++;
                s_page_reads++;
            }

            fprintf(stderr,
                    "[DEBUG]   Loaded r_loaded=%zu, s_loaded=%zu pages. "
                    "Start nested join...\n",
                    r_loaded, s_loaded);
            fflush(stderr);

            /* 현재 R묶음 × S묶음에 대해 조인 수행 */
            for (size_t ri = 0; ri < r_loaded; ++ri) {
                unsigned char *r_page = r_frames[ri].page;

                Block *r_blk = (Block *)r_page;
                unsigned char *r_data = r_blk->data;
                size_t r_data_sz = pageR - sizeof(BlockHeader);  // = sizeof(r_blk->data)

                RScanCtx rctx;
                rctx.s_frames     = s_frames;
                rctx.s_loaded     = s_loaded;
                rctx.pageS        = pageS;
                rctx.p_join_count = &join_count;
                rctx.p_rows       = &rows;
                rctx.p_rows_count = &rows_count;
                rctx.p_rows_cap   = &rows_cap;

                parse_page_records(r_data, r_data_sz, r_record_cb, &rctx);
            }

            /* 진행률 갱신 */
            if (total_pairs > 0) {
                done_pairs += (uint64_t)r_loaded * (uint64_t)s_loaded;
                if (done_pairs > total_pairs) {
                    done_pairs = total_pairs; // 방어적 보정
                }
                int percent = (int)(done_pairs * 100 / total_pairs);
                if (percent > last_percent) {
                    fprintf(stderr,
                            "Progress: %3d%% (%llu / %llu block-pairs)\n",
                            percent,
                            (unsigned long long)done_pairs,
                            (unsigned long long)total_pairs);
                    fflush(stderr);
                    last_percent = percent;
                }
            }
        }
    }

    double t1 = now_ns();
    double elapsed_ms = (t1 - t0) / 1e6;

    /* 7. 결과를 txt 파일로 저장 */
    FILE *out = fopen(out_file, "w");
    if (!out) {
        perror("fopen out_file");
        // rows free는 아래에서 공통적으로 처리
    } else {
        for (size_t i = 0; i < rows_count; ++i) {
            fputs(rows[i].line, out);
        }
        fclose(out);
    }

    fprintf(stderr, "\n=== Join Summary ===\n");
    fprintf(stderr, "Elapsed time : %.3f ms\n", elapsed_ms);
    fprintf(stderr, "R page reads : %llu\n", (unsigned long long)r_page_reads);
    fprintf(stderr, "S page reads : %llu\n", (unsigned long long)s_page_reads);
    fprintf(stderr, "Join count   : %llu\n", (unsigned long long)join_count);
    fprintf(stderr, "Stored rows  : %zu\n", rows_count);

    /* 8. 자원 해제 */
    for (size_t i = 0; i < framesR; ++i) {
        free(r_frames[i].page);
    }
    for (size_t j = 0; j < framesS; ++j) {
        free(s_frames[j].page);
    }
    free(r_frames);
    free(s_frames);
    close(fdR);
    close(fdS);

    for (size_t i = 0; i < rows_count; ++i) {
        free(rows[i].line);
    }
    free(rows);

    return 0;
}
