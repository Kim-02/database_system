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

/* 시간 차이 계산 헬퍼 */
static double diff_sec(const struct timespec *start,
                       const struct timespec *end)
{
    return (end->tv_sec  - start->tv_sec)
         + (end->tv_nsec - start->tv_nsec) / 1e9;
}

/* 간단한 64비트 문자열 해시 (FNV-1a) */
static uint64_t hash_string(const char *s)
{
    uint64_t h = 1469598103934665603ULL;           /* FNV offset basis */
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;                     /* FNV prime */
    }
    return h;
}

/* 오른쪽 테이블 한 파티션의 해시 엔트리 */
typedef struct HashNode {
    char *key;          /* 조인 키 문자열 */
    char *nonkey;       /* 오른쪽 비-키 필드들 (선행 구분자 포함, 예: "|x|y|") */
    struct HashNode *next;
} HashNode;

/*
 * 파티션드 해시 Left Join
 *  - 1단계: left/right 전체를 조인 키 해시로 P개 파티션 파일로 분할
 *  - 2단계: 각 파티션 p에 대해, R_p 를 메모리에 해시로 올리고 L_p 를 스캔하며 Left Join
 *  - 단일 스레드 구현
 */
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
        fprintf(stderr,
                "[ERROR] block_size too small: B_L=%zu, B_R=%zu\n",
                B_L, B_R);
        exit(EXIT_FAILURE);
    }

    int max_left_recs  = (int)(B_L / (sizeof(uint16_t) + MIN_REC_BYTES));
    int max_right_recs = (int)(B_R / (sizeof(uint16_t) + MIN_REC_BYTES));
    if (max_left_recs <= 0 || max_right_recs <= 0) {
        fprintf(stderr,
                "[ERROR] block_size too small for records: B_L=%zu, B_R=%zu\n",
                B_L, B_R);
        exit(EXIT_FAILURE);
    }

    /* ---- 원본 파일 크기 파악 ---- */
    struct stat stL, stR;
    size_t left_file_size  = 0;
    size_t right_file_size = 0;

    if (stat(left->filename, &stL) == 0) {
        left_file_size = (size_t)stL.st_size;
    }
    if (stat(right->filename, &stR) == 0) {
        right_file_size = (size_t)stR.st_size;
    }

    /* ---- 파티션 개수 P 결정 ----
     *  - 메모리 제한이 있으면 오른쪽 파일을 여러 파티션으로 나눔
     *  - 메모리 제한이 0이면 P=1 (단일 파티션, 사실상 일반 해시 조인)
     */
    unsigned int P = 1;
    if (g_max_memory_bytes > 0 && right_file_size > 0) {
        /* 오른쪽 한 파티션에 할당하고 싶은 목표 크기(대략) */
        size_t mem_for_right = g_max_memory_bytes / 4;   /* 전체의 1/4 정도를 오른쪽 파티션용으로 */
        if (mem_for_right < (1ULL << 20)) {              /* 너무 작으면 최소 1MB로 보정 */
            mem_for_right = (1ULL << 20);
        }
        size_t target_part_size = mem_for_right;
        if (target_part_size == 0) {
            target_part_size = 1;
        }
        P = (unsigned int)((right_file_size + target_part_size - 1) / target_part_size);
        if (P < 1)  P = 1;
        if (P > 64) P = 64;  /* 파티션 파일 수 상한 */
    }

    printf("[INFO] Partitioned hash left join\n");
    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n",
           g_max_memory_bytes, B_L, B_R);
    printf("[INFO] left_file_size=%zu, right_file_size=%zu, P=%u\n",
           left_file_size, right_file_size, P);

    /* ---- 파티션 파일 경로 및 핸들 준비 ---- */
    char **left_part_paths  = (char**)malloc(sizeof(char*) * P);
    char **right_part_paths = (char**)malloc(sizeof(char*) * P);
    if (!left_part_paths || !right_part_paths) {
        perror("malloc partition paths");
        exit(EXIT_FAILURE);
    }

    FILE **lfp = (FILE**)malloc(sizeof(FILE*) * P);
    FILE **rfp = (FILE**)malloc(sizeof(FILE*) * P);
    if (!lfp || !rfp) {
        perror("malloc partition file arrays");
        exit(EXIT_FAILURE);
    }

    size_t *right_part_rec_counts = (size_t*)calloc(P, sizeof(size_t));
    if (!right_part_rec_counts) {
        perror("calloc right_part_rec_counts");
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
        if (!lfp[p]) {
            perror("fopen left partition");
            exit(EXIT_FAILURE);
        }
        rfp[p] = fopen(right_part_paths[p], "w+");
        if (!rfp[p]) {
            perror("fopen right partition");
            exit(EXIT_FAILURE);
        }
    }

    /* ---- 1단계: 원본 left/right 를 파티션 파일로 분할 ---- */
    int left_blocks  = 0;
    int right_blocks = 0;

    /* left 분할 */
    {
        FILE *lf = fopen(left->filename, "r");
        if (!lf) {
            perror("fopen left");
            exit(EXIT_FAILURE);
        }

        Block *left_blk  = (Block*)big_alloc(B_L);
        char **left_recs = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);
        PendingLine pendL = {0};

        while (1) {
            int left_count = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(lf, B_L,
                                left_blk,
                                left_recs,
                                &left_count,
                                &pendL,
                                max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || left_count == 0) {
                break;
            }
            left_blocks++;

            for (int i = 0; i < left_count; i++) {
                char keyL[256];
                get_field(left_recs[i], left->header.key_index,
                          keyL, sizeof(keyL));

                uint64_t h = hash_string(keyL);
                unsigned int part = (unsigned int)(h % P);

                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fputs(left_recs[i], lfp[part]);
                fputc('\n', lfp[part]);
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);
            }
        }

        if (pendL.pending_line) {
            free(pendL.pending_line);
        }
        fclose(lf);
    }

    /* right 분할 */
    {
        FILE *rf = fopen(right->filename, "r");
        if (!rf) {
            perror("fopen right");
            exit(EXIT_FAILURE);
        }

        Block *right_blk  = (Block*)big_alloc(B_R);
        char **right_recs = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);
        PendingLine pendR = {0};

        while (1) {
            int right_count = 0;
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rf, B_R,
                                right_blk,
                                right_recs,
                                &right_count,
                                &pendR,
                                max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || right_count == 0) {
                break;
            }
            right_blocks++;

            for (int j = 0; j < right_count; j++) {
                char keyR[256];
                get_field(right_recs[j], right->header.key_index,
                          keyR, sizeof(keyR));

                uint64_t h = hash_string(keyR);
                unsigned int part = (unsigned int)(h % P);

                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fputs(right_recs[j], rfp[part]);
                fputc('\n', rfp[part]);
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);

                right_part_rec_counts[part]++;
            }
        }

        if (pendR.pending_line) {
            free(pendR.pending_line);
        }
        fclose(rf);
    }

    /* 파티션 파일을 읽기 위해 파일 포인터를 처음으로 돌림 */
    for (unsigned int p = 0; p < P; p++) {
        fflush(lfp[p]);
        fflush(rfp[p]);
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);
    }

    /* ---- 2단계: 파티션별 해시 Left Join ---- */
    FILE *out = fopen(output_path, "w");
    if (!out) {
        perror("fopen join output");
        exit(EXIT_FAILURE);
    }

    int nonkey_cnt = right->header.num_columns - 1;

    for (unsigned int p = 0; p < P; p++) {
        /* 이 파티션에 오른쪽 레코드가 하나도 없으면:
         * left 레코드만 NULL 채워서 출력
         */
        if (right_part_rec_counts[p] == 0) {
            FILE *lf = lfp[p];
            if (!lf) continue;

            char *line = NULL;
            size_t cap = 0;
            ssize_t nread;

            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            while ((nread = getline(&line, &cap, lf)) != -1) {
                if (nread > 0 && line[nread - 1] == '\n') {
                    line[--nread] = '\0';
                }

                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fprintf(out, "%s", line);
                for (int k = 0; k < nonkey_cnt; k++) {
                    fprintf(out, "NULL|");
                }
                fprintf(out, "\n");
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);
            }
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            free(line);
            continue;
        }

        /* ---- 오른쪽 파티션 R_p 를 메모리에 해시 테이블로 구성 ---- */
        size_t right_recs_p = right_part_rec_counts[p];
        /* 버킷 수: 2의 거듭제곱, 평균 load factor ≈ 0.5 정도 목표 */
        size_t bucket_count = 1;
        while (bucket_count < right_recs_p * 2) {
            bucket_count <<= 1;
        }
        if (bucket_count == 0) bucket_count = 1;

        HashNode **buckets = (HashNode**)calloc(bucket_count, sizeof(HashNode*));
        if (!buckets) {
            perror("calloc buckets");
            exit(EXIT_FAILURE);
        }

        /* R_p 읽어서 해시 테이블에 채우기 */
        FILE *rf = rfp[p];
        char *line = NULL;
        size_t cap = 0;
        ssize_t nread;

        struct timespec tr1, tr2;
        clock_gettime(CLOCK_MONOTONIC, &tr1);
        while ((nread = getline(&line, &cap, rf)) != -1) {
            if (nread > 0 && line[nread - 1] == '\n') {
                line[--nread] = '\0';
            }

            char key_buf[256];
            get_field(line, right->header.key_index, key_buf, sizeof(key_buf));

            char nonkey_buf[4096];
            build_nonkey_fields(right, line, nonkey_buf, sizeof(nonkey_buf));

            char *key = strdup(key_buf);
            char *nonkey = strdup(nonkey_buf);
            if (!key || !nonkey) {
                perror("strdup");
                exit(EXIT_FAILURE);
            }

            uint64_t h = hash_string(key);
            size_t idx = h & (bucket_count - 1);

            HashNode *node = (HashNode*)malloc(sizeof(HashNode));
            if (!node) {
                perror("malloc HashNode");
                exit(EXIT_FAILURE);
            }
            node->key    = key;
            node->nonkey = nonkey;
            node->next   = buckets[idx];
            buckets[idx] = node;
        }
        clock_gettime(CLOCK_MONOTONIC, &tr2);
        g_time_read += diff_sec(&tr1, &tr2);
        free(line);

        /* ---- L_p 를 스캔하면서 해시 Left Join ---- */
        FILE *lf = lfp[p];
        line = NULL;
        cap  = 0;

        clock_gettime(CLOCK_MONOTONIC, &tr1);
        while ((nread = getline(&line, &cap, lf)) != -1) {
            if (nread > 0 && line[nread - 1] == '\n') {
                line[--nread] = '\0';
            }

            char key_buf[256];
            get_field(line, left->header.key_index, key_buf, sizeof(key_buf));
            uint64_t h = hash_string(key_buf);
            size_t idx = h & (bucket_count - 1);

            int matched = 0;
            for (HashNode *node = buckets[idx]; node; node = node->next) {
                if (strcmp(key_buf, node->key) == 0) {
                    matched = 1;
                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);
                    fprintf(out, "%s%s\n", line, node->nonkey);
                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);
                }
            }

            if (!matched) {
                struct timespec tw1, tw2;
                clock_gettime(CLOCK_MONOTONIC, &tw1);
                fprintf(out, "%s", line);
                for (int k = 0; k < nonkey_cnt; k++) {
                    fprintf(out, "NULL|");
                }
                fprintf(out, "\n");
                clock_gettime(CLOCK_MONOTONIC, &tw2);
                g_time_write += diff_sec(&tw1, &tw2);
            }
        }
        clock_gettime(CLOCK_MONOTONIC, &tr2);
        g_time_read += diff_sec(&tr1, &tr2);
        free(line);

        /* 해시 테이블 메모리 해제 */
        for (size_t i = 0; i < bucket_count; i++) {
            HashNode *node = buckets[i];
            while (node) {
                HashNode *next = node->next;
                free(node->key);
                free(node->nonkey);
                free(node);
                node = next;
            }
        }
        free(buckets);

        /* 다음 파티션 처리를 위해 파일 포인터를 처음으로 되돌림 */
        fseek(lfp[p], 0, SEEK_SET);
        fseek(rfp[p], 0, SEEK_SET);
    }

    fclose(out);

    /* ---- 파티션 파일 정리 ---- */
    for (unsigned int p = 0; p < P; p++) {
        if (lfp[p]) fclose(lfp[p]);
        if (rfp[p]) fclose(rfp[p]);
        /* 임시 파일 삭제 (실험 편의를 위해 남기고 싶다면 주석 처리) */
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

    /* ---- 전체 join time 계산 ---- */
    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total_join = diff_sec(&tj_start, &tj_end);
    g_time_join = total_join - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks;
    *right_block_count_out = right_blocks;
}
