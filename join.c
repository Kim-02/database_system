#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>   // ★ 추가
#include "memory.h"
#include "table.h"
#include "blockio.h"
#include "join.h"

/* ==== 조인 단계별 시간 측정용 전역 변수 ==== */
double g_time_read  = 0.0;
double g_time_join  = 0.0;
double g_time_write = 0.0;

static double diff_sec(const struct timespec *start,
                       const struct timespec *end)
{
    return (end->tv_sec  - start->tv_sec)
         + (end->tv_nsec - start->tv_nsec) / 1e9;
}

/* B-전략: 
 *  - 오른쪽 전체가 메모리에 들어가면: 오른쪽 전체를 한 번만 읽어서 메모리에 올리고 재사용
 *  - 안 들어가면: 왼쪽을 chunk로 묶어서 메모리에 올리고, chunk마다 오른쪽을 한 번씩 스캔
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
    if (g_max_memory_bytes > 0 && g_max_memory_bytes < B_L + B_R) {
        fprintf(stderr,
                "[ERROR] max_mem_bytes (%zu) is too small: "
                "need at least left_block(%zu) + right_block(%zu)\n",
                g_max_memory_bytes, B_L, B_R);
        exit(EXIT_FAILURE);
    }

    int max_left_recs  = (int)(B_L / (sizeof(uint16_t) + MIN_REC_BYTES));
    int max_right_recs = (int)(B_R / (sizeof(uint16_t) + MIN_REC_BYTES));

    if (max_left_recs <= 0 || max_right_recs <= 0) {
        fprintf(stderr,
                "[ERROR] block_size too small: B_L=%zu, B_R=%zu\n",
                B_L, B_R);
        exit(EXIT_FAILURE);
    }

    /* 블록 1개당 필요한 메모리 추정 */
    size_t per_right_block_bytes =
        B_R
      + sizeof(Block*)
      + sizeof(int)
      + sizeof(char*) * (size_t)max_right_recs;

    size_t per_left_block_bytes =
        B_L + sizeof(char*) * (size_t)max_left_recs;

    if (g_max_memory_bytes > 0 &&
        g_max_memory_bytes <= per_left_block_bytes) {
        fprintf(stderr,
                "[ERROR] max_mem_bytes (%zu) is too small: "
                "need at least one left block + left rec ptrs (%zu bytes)\n",
                g_max_memory_bytes, per_left_block_bytes);
        exit(EXIT_FAILURE);
    }

    /* 오른쪽 파일 크기 / 블록 수 추정 */
    struct stat stR;
    size_t right_file_size = 0;
    size_t right_blocks_est = 0;

    if (stat(right->filename, &stR) == 0) {
        right_file_size = (size_t)stR.st_size;
        right_blocks_est = (right_file_size + B_R - 1) / B_R;
        if (right_blocks_est == 0) right_blocks_est = 1;
    } else {
        perror("stat right");
        fprintf(stderr, "[WARN] stat failed for %s, will estimate with runtime blocks.\n",
                right->filename);
        right_blocks_est = 0; // 나중에 runtime 기준으로만 판단
    }

    /* 오른쪽 전체를 메모리에 올릴 수 있는지 판단 */
    int right_fits_in_mem = 0;
    size_t max_right_blocks_by_mem = 0;

    if (g_max_memory_bytes == 0) {
        /* 메모리 제한 없음: 이론상 다 올릴 수 있다고 가정 */
        right_fits_in_mem = 1;
        max_right_blocks_by_mem = (right_blocks_est > 0) ? right_blocks_est : 0;
    } else {
        size_t max_bytes_for_right = g_max_memory_bytes - per_left_block_bytes;
        if (per_right_block_bytes == 0) {
            fprintf(stderr, "[ERROR] per_right_block_bytes == 0 ?\n");
            exit(EXIT_FAILURE);
        }
        max_right_blocks_by_mem = max_bytes_for_right / per_right_block_bytes;
        if (right_blocks_est > 0 &&
            max_right_blocks_by_mem >= right_blocks_est) {
            right_fits_in_mem = 1;
        }
    }

    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu\n",
           g_max_memory_bytes, B_L, B_R);
    printf("[INFO] max_left_recs=%d, max_right_recs=%d\n",
           max_left_recs, max_right_recs);
    printf("[INFO] right_file_size=%zu, est_right_blocks=%zu, "
           "per_right_block=%zu, max_right_blocks_by_mem=%zu, "
           "right_fits_in_mem=%d\n",
           right_file_size, right_blocks_est,
           per_right_block_bytes, max_right_blocks_by_mem,
           right_fits_in_mem);

    int left_blocks  = 0;
    int right_blocks = 0;

    /* ==== 케이스 1: 오른쪽 전체를 메모리에 올릴 수 있는 경우 ==== */
    if (right_fits_in_mem) {
        printf("[INFO] Strategy: FULL_RIGHT_IN_MEMORY\n");

        /* ---- 오른쪽 전체를 메모리에 로드 ---- */
        size_t capacity_blocks = (right_blocks_est > 0)
                               ? right_blocks_est
                               : (max_right_blocks_by_mem > 0 ? max_right_blocks_by_mem : 1024);

        Block **right_blks = (Block**)big_alloc(sizeof(Block*) * capacity_blocks);
        for (size_t i = 0; i < capacity_blocks; i++) {
            right_blks[i] = (Block*)big_alloc(B_R);
        }
        char **right_recs = (char**)big_alloc(
            sizeof(char*) * capacity_blocks * (size_t)max_right_recs);
        int   *right_counts = (int*)big_alloc(sizeof(int) * capacity_blocks);

        FILE *rf = fopen(right->filename, "r");
        if (!rf) {
            perror("fopen right");
            exit(EXIT_FAILURE);
        }
        PendingLine pendR = {0};

        size_t used_blocks = 0;
        while (used_blocks < capacity_blocks) {
            int rcnt;
            char **rec_ptrs_for_block =
                &right_recs[used_blocks * (size_t)max_right_recs];

            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int ok = fill_block(rf, B_R,
                                right_blks[used_blocks],
                                rec_ptrs_for_block,
                                &rcnt,
                                &pendR,
                                max_right_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok || rcnt == 0) {
                break;
            }

            right_counts[used_blocks] = rcnt;
            used_blocks++;
            right_blocks++;
        }

        if (pendR.pending_line) {
            free(pendR.pending_line);
        }
        fclose(rf);

        printf("[INFO] Loaded right blocks into memory: %zu blocks\n", used_blocks);

        /* ---- 왼쪽을 블록 단위로 읽으면서, 메모리상의 오른쪽과 조인 ---- */
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
        setvbuf(out, NULL, _IOFBF, 65536); // I/O 버퍼링 최적화 1

        Block *left_blk  = (Block*)big_alloc(B_L);
        char **left_recs = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);

        PendingLine pendL = {0};

        while (1) {
            struct timespec tr1, tr2;
            clock_gettime(CLOCK_MONOTONIC, &tr1);
            int left_count;
            int ok_left = fill_block(lf, B_L,
                                     left_blk, left_recs, &left_count, &pendL,
                                     max_left_recs);
            clock_gettime(CLOCK_MONOTONIC, &tr2);
            g_time_read += diff_sec(&tr1, &tr2);

            if (!ok_left || left_count == 0) {
                break;
            }
            left_blocks++;

            int *matched = (int*)calloc((size_t)left_count, sizeof(int));
            if (!matched) {
                perror("calloc matched");
                exit(EXIT_FAILURE);
            }

            /* 조인: 오른쪽은 메모리에 있는 블록들만 순회 */
            for (int i = 0; i < left_count; i++) {
                char keyL[256];
                get_field(left_recs[i], left->header.key_index,
                          keyL, sizeof(keyL));

                for (size_t bi = 0; bi < used_blocks; bi++) {
                    int rcnt = right_counts[bi];
                    char **base_rec_ptrs =
                        &right_recs[bi * (size_t)max_right_recs];

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

                            struct timespec tw1, tw2;
                            clock_gettime(CLOCK_MONOTONIC, &tw1);
                            fprintf(out, "%s%s\n", left_recs[i], right_nonkey);
                            clock_gettime(CLOCK_MONOTONIC, &tw2);
                            g_time_write += diff_sec(&tw1, &tw2);

                            matched[i] = 1;
                        }
                    }
                }
            }

            /* 매칭 안 된 왼쪽 레코드에 대해 NULL 채워서 출력 */
            int nonkey_cnt = right->header.num_columns - 1;
            for (int i = 0; i < left_count; i++) {
                if (!matched[i]) {
                    struct timespec tw1, tw2;
                    clock_gettime(CLOCK_MONOTONIC, &tw1);

                    fprintf(out, "%s", left_recs[i]);
                    for (int k = 0; k < nonkey_cnt; k++) {
                        fprintf(out, "NULL|");
                    }
                    fprintf(out, "\n");

                    clock_gettime(CLOCK_MONOTONIC, &tw2);
                    g_time_write += diff_sec(&tw1, &tw2);
                }
            }

            free(matched);
        }

        if (pendL.pending_line) {
            free(pendL.pending_line);
        }

        fclose(lf);
        fclose(out);
    }
    /* ==== 케이스 2: 오른쪽 전체가 메모리에 안 들어가는 경우 ==== */
    else {
        printf("[INFO] Strategy: LEFT_CHUNKED + RIGHT_SCAN_PER_CHUNK\n");

        /* 오른쪽 1블록(또는 소량)만 메모리에 두고,
           남은 메모리로 왼쪽 블록 여러 개를 chunk로 올림 */
        size_t mem_for_right = per_right_block_bytes;
        size_t mem_for_left_chunks = g_max_memory_bytes - mem_for_right;

        /* 왼쪽 1블록에 실제로 big_alloc 할 모든 것 포함:
        - 데이터 블록(B_L)
        - left_recs 내 포인터(char* * max_left_recs)
        - left_blks 포인터 배열에 대한 Block* 하나
        - left_counts 배열에 대한 int 하나
        */
        size_t bytes_per_left_block_full =
            per_left_block_bytes      // B_L + char* * max_left_recs
        + sizeof(Block*)            // left_blks[bi]
        + sizeof(int);              // left_counts[bi]

        if (mem_for_left_chunks < bytes_per_left_block_full) {
            fprintf(stderr,
                    "[ERROR] max_mem_bytes(%zu) too small for chunked join. "
                    "Need at least one full left block (%zu bytes)\n",
                    g_max_memory_bytes, bytes_per_left_block_full);
            exit(EXIT_FAILURE);
        }

        int max_left_blocks_chunk =
            (int)(mem_for_left_chunks / bytes_per_left_block_full);
        if (max_left_blocks_chunk < 1) max_left_blocks_chunk = 1;

        printf("[INFO] max_left_blocks_per_chunk=%d (bytes_per_left_block_full=%zu)\n",
            max_left_blocks_chunk, bytes_per_left_block_full);

        /* 왼쪽 chunk용 버퍼 */
        Block **left_blks = (Block**)big_alloc(sizeof(Block*) * (size_t)max_left_blocks_chunk);
        for (int i = 0; i < max_left_blocks_chunk; i++) {
            left_blks[i] = (Block*)big_alloc(B_L);
        }
        char **left_recs = (char**)big_alloc(
            sizeof(char*) * (size_t)max_left_blocks_chunk * (size_t)max_left_recs);
        int   *left_counts = (int*)big_alloc(sizeof(int) * (size_t)max_left_blocks_chunk);

        /* 오른쪽 1블록용 버퍼 */
        Block *right_blk = (Block*)big_alloc(B_R);
        char **right_recs = (char**)big_alloc(sizeof(char*) * (size_t)max_right_recs);

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
        setvbuf(out, NULL, _IOFBF, 65536); // I/O 버퍼링 최적화2

        PendingLine pendL = {0};

        while (1) {
            /* ---- 왼쪽에서 하나의 chunk(여러 블록)를 메모리에 채움 ---- */
            int chunk_blocks = 0;

            for (int bi = 0; bi < max_left_blocks_chunk; bi++) {
                int left_count;
                char **rec_ptrs_for_block =
                    &left_recs[bi * (size_t)max_left_recs];

                struct timespec tr1, tr2;
                clock_gettime(CLOCK_MONOTONIC, &tr1);
                int ok_left = fill_block(lf, B_L,
                                         left_blks[bi],
                                         rec_ptrs_for_block,
                                         &left_count,
                                         &pendL,
                                         max_left_recs);
                clock_gettime(CLOCK_MONOTONIC, &tr2);
                g_time_read += diff_sec(&tr1, &tr2);

                if (!ok_left || left_count == 0) {
                    break;
                }

                left_counts[bi] = left_count;
                chunk_blocks++;
                left_blocks++;
            }

            if (chunk_blocks == 0) {
                /* 더 이상 읽을 왼쪽 블록 없음 */
                break;
            }

            /* chunk 내 각 왼쪽 블록/레코드별 matched 플래그 */
            int **matched = (int**)malloc(sizeof(int*) * (size_t)chunk_blocks);
            if (!matched) {
                perror("malloc matched[]");
                exit(EXIT_FAILURE);
            }
            for (int bi = 0; bi < chunk_blocks; bi++) {
                matched[bi] = (int*)calloc((size_t)left_counts[bi], sizeof(int));
                if (!matched[bi]) {
                    perror("calloc matched[bi]");
                    exit(EXIT_FAILURE);
                }
            }

            /* ---- 이 chunk에 대해, 오른쪽 파일을 한 번 스캔 ---- */
            FILE *rf = fopen(right->filename, "r");
            if (!rf) {
                perror("fopen right");
                exit(EXIT_FAILURE);
            }
            PendingLine pendR = {0};

            while (1) {
                int rcnt;

                struct timespec tr1, tr2;
                clock_gettime(CLOCK_MONOTONIC, &tr1);
                int ok_right = fill_block(rf, B_R,
                                          right_blk,
                                          right_recs,
                                          &rcnt,
                                          &pendR,
                                          max_right_recs);
                clock_gettime(CLOCK_MONOTONIC, &tr2);
                g_time_read += diff_sec(&tr1, &tr2);

                if (!ok_right || rcnt == 0) {
                    break;
                }

                right_blocks++;

                /* 오른쪽 블록의 각 레코드를, chunk 내 모든 왼쪽 레코드와 비교 */
                for (int bi = 0; bi < chunk_blocks; bi++) {
                    int left_count = left_counts[bi];
                    char **left_block_recs =
                        &left_recs[bi * (size_t)max_left_recs];

                    for (int i = 0; i < left_count; i++) {
                        char keyL[256];
                        get_field(left_block_recs[i], left->header.key_index,
                                  keyL, sizeof(keyL));

                        for (int j = 0; j < rcnt; j++) {
                            char *recR = right_recs[j];
                            char keyR[256];
                            get_field(recR, right->header.key_index,
                                      keyR, sizeof(keyR));

                            if (strcmp(keyL, keyR) == 0) {
                                char right_nonkey[4096];
                                build_nonkey_fields(right,
                                                    recR,
                                                    right_nonkey,
                                                    sizeof(right_nonkey));

                                struct timespec tw1, tw2;
                                clock_gettime(CLOCK_MONOTONIC, &tw1);
                                fprintf(out, "%s%s\n", left_block_recs[i], right_nonkey);
                                clock_gettime(CLOCK_MONOTONIC, &tw2);
                                g_time_write += diff_sec(&tw1, &tw2);

                                matched[bi][i] = 1;
                            }
                        }
                    }
                }
            }

            if (pendR.pending_line) {
                free(pendR.pending_line);
            }
            fclose(rf);

            /* ---- 매칭 안 된 왼쪽 레코드들 NULL 채워서 출력 ---- */
            int nonkey_cnt = right->header.num_columns - 1;
            for (int bi = 0; bi < chunk_blocks; bi++) {
                int left_count = left_counts[bi];
                char **left_block_recs =
                    &left_recs[bi * (size_t)max_left_recs];

                for (int i = 0; i < left_count; i++) {
                    if (!matched[bi][i]) {
                        struct timespec tw1, tw2;
                        clock_gettime(CLOCK_MONOTONIC, &tw1);

                        fprintf(out, "%s", left_block_recs[i]);
                        for (int k = 0; k < nonkey_cnt; k++) {
                            fprintf(out, "NULL|");
                        }
                        fprintf(out, "\n");

                        clock_gettime(CLOCK_MONOTONIC, &tw2);
                        g_time_write += diff_sec(&tw1, &tw2);
                    }
                }
            }

            for (int bi = 0; bi < chunk_blocks; bi++) {
                free(matched[bi]);
            }
            free(matched);
        }

        if (pendL.pending_line) {
            free(pendL.pending_line);
        }

        fclose(lf);
        fclose(out);
    }

    /* ==== 공통: join time 계산 및 블록 카운트 반환 ==== */
    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total_join = diff_sec(&tj_start, &tj_end);
    g_time_join = total_join - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;

    *left_block_count_out  = left_blocks;
    *right_block_count_out = right_blocks;
}
