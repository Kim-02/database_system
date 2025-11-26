#ifndef MEMORY_H
#define MEMORY_H

#include <stddef.h>

extern size_t g_max_memory_bytes;
extern size_t g_big_alloc_bytes;

/* 기존: 한도 체크 + 누적 카운트 */
void* big_alloc(size_t sz);

/* ===== 추가: arena ===== */
typedef struct {
    unsigned char *base;
    size_t cap;
    size_t used;
} Arena;

/* cap_bytes 만큼 big_alloc으로 1회 확보 */
void  arena_init(Arena *a, size_t cap_bytes);

/* 파티션 처리 후 재사용(해제 없음) */
void  arena_reset(Arena *a);

/* arena에서 bump allocation */
void* arena_alloc(Arena *a, size_t sz);

/* arena_strdup */
char* arena_strdup(Arena *a, const char *s);

/* 디버그용 */
size_t arena_remaining(const Arena *a);

#endif
