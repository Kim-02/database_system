#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#include <time.h>

static inline double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

#endif