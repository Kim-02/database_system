#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "table.h"

static void parse_header(const char *header_str, TableHeader *hdr) {
    char buf[MAX_HEADER_LEN];
    strncpy(buf, header_str, MAX_HEADER_LEN - 1);
    buf[MAX_HEADER_LEN - 1] = '\0';

    hdr->num_columns = 0;
    hdr->key_index   = -1;

    char *token = strtok(buf, ",");
    while (token && hdr->num_columns < MAX_COLUMNS) {
        while (*token == ' ' || *token == '\t') token++;
        char *end = token + (int)strlen(token) - 1;
        while (end >= token && (*end == ' ' || *end == '\t')) {
            *end = '\0';
            end--;
        }

        strncpy(hdr->columns[hdr->num_columns].name,
                token, MAX_COL_NAME - 1);
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

void table_init(Table *tbl,
                const char *filename,
                const char *header_str,
                size_t block_size,
                const char *join_col_name)
{
    memset(tbl, 0, sizeof(*tbl));

    /* 기존 필드 */
    strncpy(tbl->filename, filename, sizeof(tbl->filename) - 1);
    tbl->filename[sizeof(tbl->filename) - 1] = '\0';

    /* 추가된 path 필드 (join.c 호환) */
    strncpy(tbl->path, filename, sizeof(tbl->path) - 1);
    tbl->path[sizeof(tbl->path) - 1] = '\0';

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
