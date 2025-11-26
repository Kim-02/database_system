#ifndef BLOCKIO_H
#define BLOCKIO_H

#include <stdio.h>
#include <stddef.h>
#include "table.h"

typedef struct {
    int used;
    int record_count;
} BlockHeader;

typedef struct {
    BlockHeader hdr;
    /* 뒤에 [uint16_t len][record bytes + '\0']... */
} Block;

/* getline 버퍼 + pending 레코드 버퍼를 재사용해서 malloc/free 변동 제거 */
typedef struct {
    /* 블록에 못 들어가서 다음 호출로 넘길 레코드 */
    char   *pending_buf;
    size_t  pending_cap;
    size_t  pending_len; /* '\n' 제거 후 길이(널 제외). 0이면 없음 */

    /* getline()이 쓰는 버퍼(재사용) */
    char   *line_buf;
    size_t  line_cap;
} PendingLine;

void pendingline_free(PendingLine *p);

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
