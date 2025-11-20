#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/resource.h>
#include "memory.h"
#include "table.h"
#include "join.h"

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

    struct timespec ts_start, ts_end;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    Table left_tbl, right_tbl;
    table_init(&left_tbl,  left_file,  left_header,  left_block,  join_col);
    table_init(&right_tbl, right_file, right_header, right_block, join_col);

    int left_blocks  = 0;
    int right_blocks = 0;
    left_join_strategyB(&left_tbl, &right_tbl, "join.txt",
                        &left_blocks, &right_blocks);

    clock_gettime(CLOCK_MONOTONIC, &ts_end);

    double elapsed =
        (ts_end.tv_sec  - ts_start.tv_sec) +
        (ts_end.tv_nsec - ts_start.tv_nsec) / 1e9;

    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    long max_rss_kb = usage.ru_maxrss;

    printf("========== STATS ==========\n");
    printf("Elapsed time : %.6f seconds\n", elapsed);
    printf("  Read time  : %.6f seconds\n", g_time_read);
    printf("  Join time  : %.6f seconds\n", g_time_join);
    printf("  Write time : %.6f seconds\n", g_time_write);
    printf("Big alloc    : %zu bytes (blocks etc.)\n", g_big_alloc_bytes);
    printf("Max RSS      : %ld KB (OS reported)\n", max_rss_kb);
    printf("Left blocks  : %d\n", left_blocks);
    printf("Right blocks : %d\n", right_blocks);

    double mem_used_pct = 0.0;
    if (g_max_memory_bytes > 0) {
        mem_used_pct =
            100.0 * (double)g_big_alloc_bytes / (double)g_max_memory_bytes;
    }
    printf("Memory usage : %zu / %zu bytes (%.2f%%)\n",
           g_big_alloc_bytes, g_max_memory_bytes, mem_used_pct);

    printf("Output file  : join.txt\n");

    return 0;
}
