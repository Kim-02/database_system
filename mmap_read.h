#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

//change page size
#define PAGESIZE 2048

typedef struct {
    uint32_t block_id;        // 블록 번호
    uint16_t remaining_space; // datad에 남은 바이트
    uint16_t record_count;    // 레코드 개수
} BlockHeader;

typedef struct {
    BlockHeader header;
    unsigned char data[PAGESIZE - sizeof(BlockHeader)];
} Block;

typedef struct {
    uint16_t count;
    uint16_t lens[PAGESIZE];
    uint32_t offs[PAGESIZE]; // data[] 기준 오프셋
} RecIndex;

//block first setting
static inline void block_init(Block *b, uint32_t id) {
    memset(b, 0, sizeof(*b));
    b->header.block_id = id;
    b->header.remaining_space = (uint16_t)sizeof(b->data);
    b->header.record_count = 0;
}

//how many bytes block used
static inline size_t block_used(const Block *b) {
    return sizeof(b->data) - b->header.remaining_space;
}

//block can fit -> block is empty or record can input block
static inline int block_can_fit(const Block *b, size_t rec_len) {
    return rec_len <= b->header.remaining_space;
}

//recindex reset 
static inline void recindex_reset(RecIndex *ri) {
    ri->count = 0;
}

//add recindex -> can not over PAGESIZE
static inline void recindex_add(RecIndex *ri, uint32_t off, uint16_t len) {
    if (ri->count < PAGESIZE) {
        ri->offs[ri->count] = off;
        ri->lens[ri->count] = len;
        ri->count++;
    }
}

//if block full -> stdout
static void flush_block_stdout(const Block *b, const RecIndex *ri) {
    // printf("=== Block #%u ===\n", b->header.block_id);
    // printf("record_count: %u, used: %zu, remaining: %u\n",
    //        b->header.record_count, block_used(b), b->header.remaining_space);

    // for (uint16_t i = 0; i < ri->count; i++) {
    //     uint32_t off = ri->offs[i];
    //     uint16_t len = ri->lens[i];
    //     // 레코드 원문 출력(널 종료 없음)
    //     fwrite(b->data + off, 1, len, stdout);
    //     fputc('\n', stdout);
    // }
}

/*
clock utils
*/
static inline double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

int mmap_read(const char* fileName){
    int fd = open(fileName, O_RDONLY);
    struct stat st;
    fstat(fd,&st);

    size_t filesize = (size_t)st.st_size;
    char* base = mmap(NULL,filesize,PROT_READ,MAP_PRIVATE,fd,0);
    close(fd);
    //block setting
    
    Block blk;
    RecIndex ri;
    uint32_t blk_id = 0;
    block_init(&blk, blk_id);
    recindex_reset(&ri);

    //max size of record
    size_t max_payload = sizeof(blk.data);

    const char *ptr = base;
    const char *end = base + filesize;

    //파일 성능 채크
    double t0 = now_ns();
    uint64_t total_records = 0;
    uint64_t total_blocks_flushed = 0;
    // 파일에서 실제로 소비한 총 바이트(개행 제외 후 레코드 길이 합산)
    uint64_t total_payload_bytes = 0;

    while (ptr < end) {
        const char *nl = memchr(ptr, '\n', (size_t)(end - ptr));
        size_t len = nl ? (size_t)(nl - ptr) : (size_t)(end - ptr);
        if (len > 0 && ptr[len - 1] == '\r') len--; // CRLF 대응

        if (len > max_payload) {
            fprintf(stderr, "Error: record (%zu B) exceeds page payload (%zu B)\n", len, max_payload);
            munmap(base, filesize);
            return 2;
        }

        // 공간 부족 시 현재 블록을 출력(flush)하고 새 블록 시작
        if (!block_can_fit(&blk, len)) {
            flush_block_stdout(&blk, &ri);
            total_blocks_flushed++;
            block_init(&blk, ++blk_id);
            recindex_reset(&ri);
        }

        // 현재 블록에 append
        uint32_t off = (uint32_t)block_used(&blk);
        memcpy(blk.data + off, ptr, len);
        blk.header.remaining_space -= (uint16_t)len;
        blk.header.record_count += 1;
        recindex_add(&ri, off, (uint16_t)len);

        total_records++;
        total_payload_bytes += (uint64_t)len;

        if (!nl) break;
        ptr = nl + 1;
    }
    if (blk.header.record_count > 0) {
        flush_block_stdout(&blk, &ri);
    }
    double t1 = now_ns();
    double elapsed_s = (t1 - t0) / 1e9;
    fprintf(stderr,
            "\n--- Disk I/O Summary of Page Size : %d ---\n"
            "File bytes          : %zu\n"
            "Payload bytes(sum)  : %llu\n"
            "Records             : %llu\n"
            "Blocks flushed      : %llu\n"
            "Elapsed             : %.3f ms\n"
            "Throughput          : %.2f MB/s (payload)\n"
            "Records per second  : %.0f rec/s\n",
            PAGESIZE,
            filesize,
            (unsigned long long)total_payload_bytes,
            (unsigned long long)total_records,
            (unsigned long long)total_blocks_flushed,
            elapsed_s * 1e3,
            (total_payload_bytes / 1e6) / elapsed_s,
            total_records / elapsed_s
    );
    munmap(base, filesize);
    return 0;
}