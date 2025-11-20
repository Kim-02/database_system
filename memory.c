#include <stdio.h>
#include <stdlib.h>
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
