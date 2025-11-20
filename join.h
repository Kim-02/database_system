#ifndef JOIN_H
#define JOIN_H

#include "table.h"

void left_join_strategyB(const Table *left,
                         const Table *right,
                         const char *output_path,
                         int *left_block_count_out,
                         int *right_block_count_out);

#endif
