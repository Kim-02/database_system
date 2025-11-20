#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include "blockio.h"

void get_field(const char *record,
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

void build_nonkey_fields(const Table *tbl,
                         const char *record,
                         char *out,
                         size_t out_sz)
{
    out[0] = '\0';

    int first = 1;
    char buf[512];

    for (int i = 0; i < tbl->header.num_columns; i++) {
        if (i == tbl->header.key_index)
            continue;

        get_field(record, i, buf, sizeof(buf));

        if (!first) {
            strncat(out, "|", out_sz - strlen(out) - 1);
        }
        strncat(out, buf, out_sz - strlen(out) - 1);
        first = 0;
    }
    strncat(out, "|", out_sz - strlen(out) - 1);
}

int fill_block(FILE *fp,
               size_t block_size,
               Block *blk,
               char **rec_ptrs,
               int *out_count,
               PendingLine *pend,
               int max_recs)
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
                break;
            }
            src = line;
            len = nread;
        }

        while (len > 0 &&
               (src[len-1] == '\n' || src[len-1] == '\r')) {
            src[--len] = '\0';
        }

        size_t rec_len = (size_t)len + 1;
        size_t need    = sizeof(uint16_t) + rec_len;

        if (blk->hdr.used + (int)need > (int)block_size) {
            if (blk->hdr.record_count == 0) {
                fprintf(stderr,
                        "[ERROR] record too large for block_size=%zu bytes\n",
                        block_size);
                exit(EXIT_FAILURE);
            }

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

        if (*out_count >= max_recs) {
            fprintf(stderr,
                    "[ERROR] Too many records in one block. "
                    "max_recs=%d, block_size=%zu\n",
                    max_recs, block_size);
            exit(EXIT_FAILURE);
        }

        char *p = (char*)blk + blk->hdr.used;
        uint16_t len16 = (uint16_t)rec_len;
        memcpy(p, &len16, sizeof(uint16_t));
        memcpy(p + sizeof(uint16_t), src, rec_len);

        rec_ptrs[*out_count] = p + sizeof(uint16_t);
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
