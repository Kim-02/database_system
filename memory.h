#ifndef MEMORY_H
#define MEMORY_H

#include <stddef.h>

extern size_t g_max_memory_bytes;
extern size_t g_big_alloc_bytes;

void* big_alloc(size_t sz);

#endif
