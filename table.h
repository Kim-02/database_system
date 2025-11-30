#ifndef TABLE_H
#define TABLE_H

#include <stddef.h>

#define MAX_COLUMNS    64
#define MAX_HEADER_LEN 256
#define MAX_COL_NAME   64
#define MIN_REC_BYTES  2

typedef struct {
    char name[MAX_COL_NAME];
} Column;

typedef struct {
    int    num_columns;
    int    key_index;
    Column columns[MAX_COLUMNS];
} TableHeader;

typedef struct {
    /* 기존 코드 호환용 */
    char filename[256];

    /* join.c 등에서 tbl->path 로 접근하는 코드 호환용 */
    char path[256];

    TableHeader header;
    size_t block_size;
} Table;

void table_init(Table *tbl,
                const char *filename,
                const char *header_str,
                size_t block_size,
                const char *join_col_name);

#endif
