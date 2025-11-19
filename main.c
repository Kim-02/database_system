#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/resource.h>

#define MAX_COLUMNS             64
#define MAX_HEADER_LEN          256
#define MAX_COL_NAME            64
#define MAX_RECORDS_PER_BLOCK   256   /* 블록당 최대 레코드 수 (필요시 늘리면 됨) */
#define MAX_RIGHT_BLOCKS_LIMIT  1024   /* 너무 큰 K 방지용 상한 */

static size_t g_max_memory_bytes = 0;   /* 설정된 메모리 상한 */
static size_t g_big_alloc_bytes  = 0;   /* big_alloc으로 할당된 총량 */

/* ==========================
 * big_alloc: 메모리 상한 체크용
 * ========================== */
static void* big_alloc(size_t sz) {
    if (g_max_memory_bytes > 0 &&
        g_big_alloc_bytes + sz > g_max_memory_bytes) {
        fprintf(stderr,
                "[ERROR] Memory limit exceeded: "
                "request=%zu, used=%zu, limit=%zu\n",
                sz, g_big_alloc_bytes, g_max_memory_bytes);
        exit(EXIT_FAILURE);
    }
    void *p = malloc(sz);
    if (!p) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    g_big_alloc_bytes += sz;
    return p;
}

/* ==========================
 * 헤더(컬럼 정보)
 * ========================== */
typedef struct {
    char name[MAX_COL_NAME];
} Column;

typedef struct {
    int   num_columns;
    int   key_index;                 /* 조인 키의 인덱스 */
    Column columns[MAX_COLUMNS];
} TableHeader;

static void parse_header(const char *header_str, TableHeader *hdr) {
    char buf[MAX_HEADER_LEN];
    strncpy(buf, header_str, MAX_HEADER_LEN - 1);
    buf[MAX_HEADER_LEN - 1] = '\0';

    hdr->num_columns = 0;
    hdr->key_index   = -1;

    char *token = strtok(buf, ",");
    while (token && hdr->num_columns < MAX_COLUMNS) {
        /* 앞뒤 공백 제거 */
        while (*token == ' ' || *token == '\t') token++;
        char *end = token + strlen(token) - 1;
        while (end >= token && (*end == ' ' || *end == '\t')) {
            *end = '\0';
            end--;
        }

        strncpy(hdr->columns[hdr->num_columns].name,
                token,
                MAX_COL_NAME - 1);
        hdr->columns[hdr->num_columns].name[MAX_COL_NAME - 1] = '\0';
        hdr->num_columns++;

        token = strtok(NULL, ",");
    }
}

static int find_column_index(const TableHeader *hdr,
                             const char *join_col_name)
{
    for (int i = 0; i < hdr->num_columns; i++) {
        if (strcmp(hdr->columns[i].name, join_col_name) == 0)
            return i;
    }
    return -1;
}

/* ==========================
 * 블록 구조 (비신장 가변길이)
 * ========================== */
typedef struct {
    int used;         /* 블록 내 사용된 바이트 수 */
    int record_count; /* 레코드 개수 */
} BlockHeader;

typedef struct {
    BlockHeader hdr;
    /* 뒤에 실제 데이터: [uint16_t len][record bytes + '\0']... */
} Block;

/* ==========================
 * 테이블 메타 정보
 * ========================== */
typedef struct {
    char filename[256];
    TableHeader header;
    size_t block_size;   /* 비신장 가변길이 블록 크기 */
} Table;

/* ==========================
 * 레코드 유틸
 * ========================== */

/* record 문자열에서 index번째 필드를 추출 (0-based) */
static void get_field(const char *record,
                      int index,
                      char *out,
                      size_t out_sz)
{
    int   cur_idx = 0;
    size_t start = 0;
    size_t len   = strlen(record);

    out[0] = '\0';

    for (size_t i = 0; i <= len; i++) {
        if (record[i] == '|' || record[i] == '\0') {
            if (cur_idx == index) {
                size_t field_len = i - start;
                if (field_len >= out_sz)
                    field_len = out_sz - 1;
                memcpy(out, record + start, field_len);
                out[field_len] = '\0';
                return;
            }
            cur_idx++;
            start = i + 1;
        }
    }
    out[0] = '\0';
}

/* 오른쪽 테이블에서 key_index를 제외한 나머지 필드를
   "필드1|필드2|...|" 형태로 out에 구성 */
static void build_nonkey_fields(const Table *tbl,
                                const char *record,
                                char *out,
                                size_t out_sz)
{
    out[0] = '\0';

    int first = 1;
    char buf[512];

    for (int i = 0; i < tbl->header.num_columns; i++) {
        if (i == tbl->header.key_index)
            continue;   /* 조인 키 열은 제외 */

        get_field(record, i, buf, sizeof(buf));

        if (!first) {
            strncat(out, "|", out_sz - strlen(out) - 1);
        }
        strncat(out, buf, out_sz - strlen(out) - 1);
        first = 0;
    }

    /* 마지막에 trailing '|' 하나 추가 */
    strncat(out, "|", out_sz - strlen(out) - 1);
}

/* ==========================
 * 테이블 초기화
 * ========================== */
static void table_init(Table *tbl,
                       const char *filename,
                       const char *header_str,
                       size_t block_size,
                       const char *join_col_name)
{
    memset(tbl, 0, sizeof(*tbl));
    strncpy(tbl->filename, filename, sizeof(tbl->filename) - 1);
    tbl->filename[sizeof(tbl->filename) - 1] = '\0';

    parse_header(header_str, &tbl->header);
    int key_idx = find_column_index(&tbl->header, join_col_name);
    if (key_idx < 0) {
        fprintf(stderr,
                "[ERROR] join column '%s' not found in header of %s\n",
                join_col_name, filename);
        exit(EXIT_FAILURE);
    }
    tbl->header.key_index = key_idx;
    tbl->block_size       = block_size;
}

/* ==========================
 * 스트리밍 블록 로딩용
 * ========================== */
typedef struct {
    char *pending_line;   /* 블록에 다 못 넣고 남은 1줄 */
} PendingLine;

/* 파일에서 다음 블록을 채움.
   - block_size를 넘지 않도록 비신장 가변길이 방식으로 레코드들을 넣음.
   - rec_ptrs[i]에 i번째 레코드 시작 포인터 저장.
   - out_count에 레코드 개수 반환.
   - PendingLine을 이용해 남은 1줄을 다음 블록으로 넘김.

   반환:
   - 1: 적어도 한 개의 레코드를 블록에 넣음.
   - 0: 더 이상 넣을 레코드 없음 (EOF + pending 없음).
*/
static int fill_block(FILE *fp,
                      size_t block_size,
                      Block *blk,
                      char **rec_ptrs,
                      int *out_count,
                      PendingLine *pend)
{
    blk->hdr.used         = sizeof(BlockHeader);
    blk->hdr.record_count = 0;
    *out_count            = 0;

    int got_any = 0;
    char *line = NULL;
    size_t cap = 0;
    ssize_t nread;

    while (1) {
        char   *src = NULL;
        ssize_t len;
        int     from_pending = 0;

        if (pend->pending_line) {
            src = pend->pending_line;
            len = (ssize_t)strlen(src);
            pend->pending_line = NULL;
            from_pending = 1;
        } else {
            nread = getline(&line, &cap, fp);
            if (nread == -1) {
                break;  /* EOF */
            }
            src = line;
            len = nread;
        }

        /* 개행 제거 */
        while (len > 0 &&
               (src[len-1] == '\n' || src[len-1] == '\r')) {
            src[--len] = '\0';
        }

        size_t rec_len = (size_t)len + 1;            /* '\0' 포함 */
        size_t need    = sizeof(uint16_t) + rec_len; /* 길이 + 문자열 */

        if (blk->hdr.used + (int)need > (int)block_size) {
            if (blk->hdr.record_count == 0) {
                fprintf(stderr,
                        "[ERROR] record too large for block_size=%zu bytes\n",
                        block_size);
                exit(EXIT_FAILURE);
            }

            /* 이 레코드는 다음 블록에서 사용하도록 pending에 저장 */
            pend->pending_line = strdup(src);
            if (!pend->pending_line) {
                perror("strdup");
                exit(EXIT_FAILURE);
            }

            got_any = 1;
            if (from_pending) {
                free(src);
            }
            break;
        }

        if (*out_count >= MAX_RECORDS_PER_BLOCK) {
            fprintf(stderr,
                    "[ERROR] Too many records in one block. "
                    "Increase MAX_RECORDS_PER_BLOCK.\n");
            exit(EXIT_FAILURE);
        }

        char *p = (char*)blk + blk->hdr.used;
        uint16_t len16 = (uint16_t)rec_len;
        memcpy(p, &len16, sizeof(uint16_t));
        memcpy(p + sizeof(uint16_t), src, rec_len); /* '\0' 포함 */

        rec_ptrs[*out_count] = p + sizeof(uint16_t);  /* record 시작 위치 */
        (*out_count)++;

        blk->hdr.used         += (int)need;
        blk->hdr.record_count += 1;

        got_any = 1;

        if (from_pending) {
            free(src);
        }
    }

    if (line) {
        free(line);
    }

    return got_any;
}

/* ==========================
 * 전략 B:
 *  - 왼쪽: 1블록
 *  - 오른쪽: 메모리 한도 내에서 최대 K블록
 *  - LEFT OUTER JOIN 유지
 * ========================== */
static void left_join_strategyB(const Table *left,
                                const Table *right,
                                const char *output_path,
                                int *left_block_count_out,
                                int *right_block_count_out)
{
    size_t B_L = left->block_size;
    size_t B_R = right->block_size;

    if (g_max_memory_bytes > 0 && g_max_memory_bytes < B_L + B_R) {
        fprintf(stderr,
                "[ERROR] max_mem_bytes (%zu) is too small: "
                "need at least left_block(%zu) + right_block(%zu)\n",
                g_max_memory_bytes, B_L, B_R);
        exit(EXIT_FAILURE);
    }

    /* 오른쪽 1블록당 실제로 필요한 big_alloc 메모리(데이터 + 포인터들) 추정 */
    size_t per_right_block_bytes =
        B_R
    + sizeof(Block*)                    /* right_blks[i] 포인터 슬롯 하나 */
    + sizeof(int)                       /* right_counts[i] 하나 */
    + sizeof(char*) * MAX_RECORDS_PER_BLOCK; /* right_recs에서 이 블록이 차지하는 포인터들 */

    /* 왼쪽 블록 한 개(B_L)를 뺀 나머지에서 오른쪽 K블록을 넣을 수 있는 최대 개수 */
    size_t max_bytes_for_right =
        (g_max_memory_bytes > B_L) ? (g_max_memory_bytes - B_L) : 0;

    int max_right_blocks = 1;
    if (g_max_memory_bytes > 0) {
        if (per_right_block_bytes == 0) {
            fprintf(stderr, "[ERROR] per_right_block_bytes == 0 ?\n");
            exit(EXIT_FAILURE);
        }
        max_right_blocks = (int)(max_bytes_for_right / per_right_block_bytes);
        if (max_right_blocks < 1) {
            fprintf(stderr,
                    "[ERROR] Not enough memory even for one right block: "
                    "max_mem=%zu, per_right_block=%zu\n",
                    g_max_memory_bytes, per_right_block_bytes);
            exit(EXIT_FAILURE);
        }
    }

    /* 디버깅용 출력 */
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu, "
        "per_right_block=%zu, K(max_right_blocks)=%d\n",
        g_max_memory_bytes, B_L, B_R, per_right_block_bytes, max_right_blocks);


    /* 왼쪽은 1블록 버퍼 */
    Block *left_blk = (Block*)big_alloc(B_L);
    char  *left_recs[MAX_RECORDS_PER_BLOCK];

    /* 오른쪽은 K블록 버퍼 */
    Block **right_blks = (Block**)big_alloc(sizeof(Block*) * (size_t)max_right_blocks);
    for (int i = 0; i < max_right_blocks; i++) {
        right_blks[i] = (Block*)big_alloc(B_R);
    }

    /* 각 오른쪽 블록의 레코드 포인터 배열(2차원처럼 사용) */
    char **right_recs = (char**)big_alloc(
        sizeof(char*) * (size_t)max_right_blocks * (size_t)MAX_RECORDS_PER_BLOCK);
    int   *right_counts = (int*)big_alloc(sizeof(int) * (size_t)max_right_blocks);

    FILE *lf = fopen(left->filename, "r");
    if (!lf) {
        perror("fopen left");
        exit(EXIT_FAILURE);
    }

    FILE *out = fopen(output_path, "w");
    if (!out) {
        perror("fopen join.txt");
        exit(EXIT_FAILURE);
    }

    PendingLine pendL = {0};
    int left_blocks  = 0;
    int right_blocks = 0;

    int left_count;

    /* 외부 루프: 왼쪽 테이블을 블록 단위로 1개씩 처리 */
    while (fill_block(lf, B_L,
                      left_blk, left_recs, &left_count, &pendL))
    {
        if (left_count == 0) {
            break;
        }
        left_blocks++;

        /* LEFT JOIN용 매칭 여부 배열 (이 왼쪽 블록에 대해 전체 오른쪽을 본 후 판단) */
        int *matched = (int*)calloc((size_t)left_count, sizeof(int));
        if (!matched) {
            perror("calloc matched");
            exit(EXIT_FAILURE);
        }

        /* 오른쪽 파일을 매번 처음부터 열어서,
           메모리 한도에서 가능한 K블록씩 읽어가며 조인 */
        FILE *rf = fopen(right->filename, "r");
        if (!rf) {
            perror("fopen right");
            exit(EXIT_FAILURE);
        }
        PendingLine pendR = {0};

        while (1) {
            int loaded_blocks = 0;

            /* 오른쪽에서 최대 max_right_blocks 개의 블록을 메모리에 채움 */
            for (int bi = 0; bi < max_right_blocks; bi++) {
                int rcnt;
                char **rec_ptrs_for_block =
                    &right_recs[bi * MAX_RECORDS_PER_BLOCK];

                int ok = fill_block(rf, B_R,
                                    right_blks[bi],
                                    rec_ptrs_for_block,
                                    &rcnt,
                                    &pendR);
                if (!ok) {
                    break;  /* 더 이상 읽을 블록 없음 (EOF + pending 없음) */
                }
                if (rcnt == 0) {
                    break;  /* 안전상 */
                }

                right_counts[bi] = rcnt;
                loaded_blocks++;
                right_blocks++;
            }

            if (loaded_blocks == 0) {
                break;  /* 오른쪽을 모두 읽음 */
            }

            /* 현재 왼쪽 블록 vs 오른쪽 loaded_blocks 개 블록 조인 */
            for (int i = 0; i < left_count; i++) {
                char keyL[256];
                get_field(left_recs[i], left->header.key_index,
                          keyL, sizeof(keyL));

                for (int bi = 0; bi < loaded_blocks; bi++) {
                    int rcnt = right_counts[bi];
                    char **base_rec_ptrs =
                        &right_recs[bi * MAX_RECORDS_PER_BLOCK];

                    for (int j = 0; j < rcnt; j++) {
                        char *recR = base_rec_ptrs[j];
                        char keyR[256];
                        get_field(recR, right->header.key_index,
                                  keyR, sizeof(keyR));

                        if (strcmp(keyL, keyR) == 0) {
                            char right_nonkey[4096];
                            build_nonkey_fields(right,
                                                recR,
                                                right_nonkey,
                                                sizeof(right_nonkey));
                            fprintf(out, "%s%s\n", left_recs[i], right_nonkey);
                            matched[i] = 1;
                        }
                    }
                }
            }
        }

        if (pendR.pending_line) {
            free(pendR.pending_line);
        }
        fclose(rf);

        /* LEFT JOIN: 오른쪽 전체를 다 본 후에도 매칭 안 된 왼쪽 레코드 처리 */
        int nonkey_cnt = right->header.num_columns - 1;
        for (int i = 0; i < left_count; i++) {
            if (!matched[i]) {
                fprintf(out, "%s", left_recs[i]);
                for (int k = 0; k < nonkey_cnt; k++) {
                    fprintf(out, "NULL|");
                }
                fprintf(out, "\n");
            }
        }

        free(matched);
    }

    if (pendL.pending_line) {
        free(pendL.pending_line);
    }

    fclose(lf);
    fclose(out);

    *left_block_count_out  = left_blocks;
    *right_block_count_out = right_blocks;
}

/* ==========================
 * 메인
 * ========================== */
int main(int argc, char *argv[]) {
    if (argc < 9) {
        fprintf(stderr,
            "Usage:\n"
            "  %s max_mem_mb "
            "left.tbl \"left_header\" left_block_size "
            "right.tbl \"right_header\" right_block_size "
            "join_column_name\n",
            argv[0]);
        return EXIT_FAILURE;
    }

    /* 1. 최대 메모리 설정 */
    long max_mem_mb = strtol(argv[1], NULL, 10);
    if (max_mem_mb <= 0) {
        fprintf(stderr, "max_mem_mb must be > 0\n");
        return EXIT_FAILURE;
    }
    g_max_memory_bytes = (size_t)max_mem_mb * 1024ULL * 1024ULL;

    const char *left_file   = argv[2];
    const char *left_header = argv[3];
    size_t      left_block  = (size_t)strtoul(argv[4], NULL, 10);

    const char *right_file   = argv[5];
    const char *right_header = argv[6];
    size_t      right_block  = (size_t)strtoul(argv[7], NULL, 10);

    const char *join_col     = argv[8];

    /* 시간 측정 시작 */
    struct timespec ts_start, ts_end;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    /* 테이블 메타데이터 초기화 */
    Table left_tbl, right_tbl;
    table_init(&left_tbl,  left_file,  left_header,  left_block,  join_col);
    table_init(&right_tbl, right_file, right_header, right_block, join_col);

    /* 전략 B: 왼쪽 1블록 + 오른쪽 K블록 (메모리 예산 내) */
    int left_blocks  = 0;
    int right_blocks = 0;
    left_join_strategyB(&left_tbl, &right_tbl, "join.txt",
                        &left_blocks, &right_blocks);

    /* 시간 측정 종료 */
    clock_gettime(CLOCK_MONOTONIC, &ts_end);

    double elapsed =
        (ts_end.tv_sec  - ts_start.tv_sec) +
        (ts_end.tv_nsec - ts_start.tv_nsec) / 1e9;

    /* 메모리 사용량 (대략) */
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    long max_rss_kb = usage.ru_maxrss;   /* Linux: KB 단위 */

    printf("========== STATS ==========\n");
    printf("Elapsed time : %.6f seconds\n", elapsed);
    printf("Big alloc    : %zu bytes (blocks etc.)\n", g_big_alloc_bytes);
    printf("Max RSS      : %ld KB (OS reported)\n", max_rss_kb);
    printf("Left blocks  : %d\n", left_blocks);
    printf("Right blocks : %d\n", right_blocks);
    printf("Output file  : join.txt\n");

    return 0;
}
