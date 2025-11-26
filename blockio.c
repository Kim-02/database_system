#define _GNU_SOURCE
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h> /* ssize_t */
#include "blockio.h"

void pendingline_free(PendingLine *p)
{
    if (!p) return;
    free(p->pending_buf);
    free(p->line_buf);
    memset(p, 0, sizeof(*p));
}

void get_field(const char *record,
               int index,
               char *out,
               size_t out_sz)
{
    int cur_idx = 0;
    size_t start = 0;
    size_t len = strlen(record);

    if (out_sz == 0) return;
    out[0] = '\0';

    for (size_t i = 0; i <= len; i++) {
        if (record[i] == '|' || record[i] == '\0') {
            if (cur_idx == index) {
                size_t field_len = i - start;
                if (field_len >= out_sz) field_len = out_sz - 1;
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

static inline void append_bytes(char *out, size_t out_sz, size_t *pos,
                                const char *src, size_t n)
{
    if (*pos >= out_sz) return;
    size_t room = out_sz - *pos - 1; /* keep NUL */
    if (n > room) n = room;
    if (n > 0) {
        memcpy(out + *pos, src, n);
        *pos += n;
        out[*pos] = '\0';
    }
}

void build_nonkey_fields(const Table *tbl,
                         const char *record,
                         char *out,
                         size_t out_sz)
{
    if (out_sz == 0) return;
    size_t pos = 0;
    out[0] = '\0';

    char buf[512];

    for (int i = 0; i < tbl->header.num_columns; i++) {
        if (i == tbl->header.key_index) continue;

        get_field(record, i, buf, sizeof(buf));
        append_bytes(out, out_sz, &pos, buf, strlen(buf));
        append_bytes(out, out_sz, &pos, "|", 1);
    }
}

static void pending_set(PendingLine *pend, const char *s, size_t len)
{
    /* len: newline 제거된 길이(널 제외) */
    if (pend->pending_cap < len + 1) {
        size_t newcap = pend->pending_cap ? pend->pending_cap : 256;
        while (newcap < len + 1) newcap *= 2;
        char *nb = (char*)realloc(pend->pending_buf, newcap);
        if (!nb) {
            perror("realloc(pending_buf)");
            exit(EXIT_FAILURE);
        }
        pend->pending_buf = nb;
        pend->pending_cap = newcap;
    }
    memcpy(pend->pending_buf, s, len);
    pend->pending_buf[len] = '\0';
    pend->pending_len = len;
}

int fill_block(FILE *fp,
               size_t block_size,
               Block *blk,
               char **rec_ptrs,
               int *out_count,
               PendingLine *pend,
               int max_recs)
{
    blk->hdr.used         = (int)sizeof(BlockHeader);
    blk->hdr.record_count = 0;
    *out_count            = 0;

    int got_any = 0;

    while (1) {
        char   *src = NULL;
        ssize_t len = 0;

        if (!pend) {
            fprintf(stderr, "[ERROR] PendingLine* pend must not be NULL\n");
            exit(EXIT_FAILURE);
        }

        if (pend->pending_len > 0) {
            src = pend->pending_buf;
            len = (ssize_t)pend->pending_len;
            pend->pending_len = 0;
        } else {
            ssize_t nread = getline(&pend->line_buf, &pend->line_cap, fp);
            if (nread == -1) break;
            src = pend->line_buf;
            len = nread;
        }

        while (len > 0 && (src[len - 1] == '\n' || src[len - 1] == '\r')) {
            src[--len] = '\0';
        }

        size_t rec_len = (size_t)len + 1;             /* '\0' 포함 */
        size_t need    = sizeof(uint16_t) + rec_len;  /* [len16][record] */

        if ((size_t)blk->hdr.used + need > block_size) {
            if (blk->hdr.record_count == 0) {
                fprintf(stderr,
                        "[ERROR] record too large for block_size=%zu bytes\n",
                        block_size);
                exit(EXIT_FAILURE);
            }
            pending_set(pend, src, (size_t)len);
            got_any = 1;
            break;
        }

        if (*out_count >= max_recs) {
            fprintf(stderr,
                    "[ERROR] Too many records in one block. max_recs=%d, block_size=%zu\n",
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
    }

    return got_any;
}
