#ifndef BLOCKIO_H
#define BLOCKIO_H

#include <stdio.h>
#include "table.h"

typedef struct {
    int used;
    int record_count;
} BlockHeader;

typedef struct {
    BlockHeader hdr;
    /* 뒤에 [uint16_t len][record bytes + '\0']... */
} Block;

typedef struct {
    char *pending_line;
} PendingLine;

void get_field(const char *record,
               int index,
               char *out,
               size_t out_sz);

void build_nonkey_fields(const Table *tbl,
                         const char *record,
                         char *out,
                         size_t out_sz);

int fill_block(FILE *fp,
               size_t block_size,
               Block *blk,
               char **rec_ptrs,
               int *out_count,
               PendingLine *pend,
               int max_recs);

#endif
