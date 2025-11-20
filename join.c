#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
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

    size_t per_right_block_bytes =
        B_R
      + sizeof(Block*)
      + sizeof(int)
      + sizeof(char*) * (size_t)max_right_recs;

    size_t left_fixed_bytes =
        B_L + sizeof(char*) * (size_t)max_left_recs;

    if (g_max_memory_bytes <= left_fixed_bytes) {
        fprintf(stderr,
                "[ERROR] max_mem_bytes (%zu) is too small: "
                "need at least left block + left rec ptrs (%zu bytes)\n",
                g_max_memory_bytes, left_fixed_bytes);
        exit(EXIT_FAILURE);
    }

    size_t max_bytes_for_right = g_max_memory_bytes - left_fixed_bytes;

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

    printf("[INFO] max_mem=%zu, B_L=%zu, B_R=%zu, "
           "per_right_block=%zu, K(max_right_blocks)=%d\n",
           g_max_memory_bytes, B_L, B_R, per_right_block_bytes, max_right_blocks);

    Block *left_blk  = (Block*)big_alloc(B_L);
    char **left_recs = (char**)big_alloc(sizeof(char*) * (size_t)max_left_recs);

    Block **right_blks = (Block**)big_alloc(sizeof(Block*) * (size_t)max_right_blocks);
    for (int i = 0; i < max_right_blocks; i++) {
        right_blks[i] = (Block*)big_alloc(B_R);
    }

    char **right_recs = (char**)big_alloc(
        sizeof(char*) * (size_t)max_right_blocks * (size_t)max_right_recs);
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
    while (1) {
        struct timespec tr1, tr2;
        clock_gettime(CLOCK_MONOTONIC, &tr1);
        int ok_left = fill_block(lf, B_L,
                                 left_blk, left_recs, &left_count, &pendL,
                                 max_left_recs);
        clock_gettime(CLOCK_MONOTONIC, &tr2);
        g_time_read += diff_sec(&tr1, &tr2);

        if (!ok_left) {
            break;
        }
        if (left_count == 0) {
            break;
        }
        left_blocks++;

        int *matched = (int*)calloc((size_t)left_count, sizeof(int));
        if (!matched) {
            perror("calloc matched");
            exit(EXIT_FAILURE);
        }

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
                    &right_recs[bi * max_right_recs];

                struct timespec tr1, tr2;
                clock_gettime(CLOCK_MONOTONIC, &tr1);
                int ok = fill_block(rf, B_R,
                                    right_blks[bi],
                                    rec_ptrs_for_block,
                                    &rcnt,
                                    &pendR,
                                    max_right_recs);
                clock_gettime(CLOCK_MONOTONIC, &tr2);
                g_time_read += diff_sec(&tr1, &tr2);

                if (!ok) {
                    break;
                }
                if (rcnt == 0) {
                    break;
                }

                right_counts[bi] = rcnt;
                loaded_blocks++;
                right_blocks++;
            }

            if (loaded_blocks == 0) {
                break;
            }

            for (int i = 0; i < left_count; i++) {
                char keyL[256];
                get_field(left_recs[i], left->header.key_index,
                          keyL, sizeof(keyL));

                for (int bi = 0; bi < loaded_blocks; bi++) {
                    int rcnt = right_counts[bi];
                    char **base_rec_ptrs =
                        &right_recs[bi * max_right_recs];

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
        }

        if (pendR.pending_line) {
            free(pendR.pending_line);
        }
        fclose(rf);

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

    clock_gettime(CLOCK_MONOTONIC, &tj_end);
    double total_join = diff_sec(&tj_start, &tj_end);
    g_time_join = total_join - g_time_read - g_time_write;
    if (g_time_join < 0.0) g_time_join = 0.0;
    
    *left_block_count_out  = left_blocks;
    *right_block_count_out = right_blocks;
}
