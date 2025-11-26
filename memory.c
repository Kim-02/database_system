#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "memory.h"

size_t g_max_memory_bytes = 0;
size_t g_big_alloc_bytes  = 0;

void* big_alloc(size_t sz) {
    if (g_max_memory_bytes > 0 &&
        g_big_alloc_bytes + sz > g_max_memory_bytes) {
        fprintf(stderr,
                "[ERROR] Memory limit exceeded: "
                "request=%zu, used=%zu, limit=%zu\n",
                sz, g_big_alloc_bytes, g_max_memory_bytes);
        exit(EXIT_FAILURE);
    }
    void *p = malloc(sz);
    if (!p) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    g_big_alloc_bytes += sz;
    return p;
}

/* ===== arena ===== */
static inline size_t align8(size_t x) { return (x + 7u) & ~7u; }

void arena_init(Arena *a, size_t cap_bytes) {
    if (!a) return;
    if (cap_bytes == 0) cap_bytes = 1;
    a->base = (unsigned char*)big_alloc(cap_bytes);
    a->cap  = cap_bytes;
    a->used = 0;
}

void arena_reset(Arena *a) {
    if (!a) return;
    a->used = 0;
}

void* arena_alloc(Arena *a, size_t sz) {
    if (!a || !a->base) {
        fprintf(stderr, "[ERROR] arena not initialized\n");
        exit(EXIT_FAILURE);
    }
    size_t off = align8(a->used);
    if (off + sz > a->cap) {
        fprintf(stderr,
                "[ERROR] arena out of memory: request=%zu, used=%zu, cap=%zu\n",
                sz, a->used, a->cap);
        exit(EXIT_FAILURE);
    }
    void *p = a->base + off;
    a->used = off + sz;
    return p;
}

char* arena_strdup(Arena *a, const char *s) {
    if (!s) s = "";
    size_t n = strlen(s) + 1;
    char *p = (char*)arena_alloc(a, n);
    memcpy(p, s, n);
    return p;
}

size_t arena_remaining(const Arena *a) {
    if (!a) return 0;
    size_t off = align8(a->used);
    return (off >= a->cap) ? 0 : (a->cap - off);
}
