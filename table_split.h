#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

// change page size (예: 1024, 2048, 4096 등으로 바꿔가며 실험)
#define PAGESIZE 4096

typedef struct {
    uint32_t block_id;        // 블록 번호
    uint16_t remaining_space; // data에 남은 바이트
    uint16_t record_count;    // 레코드 개수
} BlockHeader;

typedef struct {
    BlockHeader header;
    unsigned char data[PAGESIZE - sizeof(BlockHeader)];
} Block;

typedef struct {
    uint16_t count;
    uint16_t lens[PAGESIZE];
    uint32_t offs[PAGESIZE]; // data[] 기준 오프셋 (현재는 디버깅/확장용)
} RecIndex;

// block 초기화
static inline void block_init(Block *b, uint32_t id) {
    memset(b, 0, sizeof(*b));
    b->header.block_id = id;
    b->header.remaining_space = (uint16_t)sizeof(b->data);
    b->header.record_count = 0;
}

// block에서 사용된 바이트 수
static inline size_t block_used(const Block *b) {
    return sizeof(b->data) - b->header.remaining_space;
}

// 현재 block에 rec_len 크기의 레코드가 들어갈 수 있는지
static inline int block_can_fit(const Block *b, size_t rec_len) {
    return rec_len <= b->header.remaining_space;
}

// RecIndex 초기화 (현재는 메모리 상에서만 사용)
static inline void recindex_reset(RecIndex *ri) {
    ri->count = 0;
}

// RecIndex에 레코드 정보 추가 (디버깅/확장용)
static inline void recindex_add(RecIndex *ri, uint32_t off, uint16_t len) {
    if (ri->count < PAGESIZE) {
        ri->offs[ri->count] = off;
        ri->lens[ri->count] = len;
        ri->count++;
    }
}

/*
 clock utils
*/
static inline double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

/*
 * 입력 경로에서 파일 이름(basename)만 추출
 * 예: "/path/to/orders.tbl" -> "orders.tbl"
 */
static const char* get_basename(const char *path) {
    const char *p = strrchr(path, '/');
    if (p) return p + 1;
    return path;
}

/*
 * 하나의 Block 전체를 paged 파일에 그대로 기록
 * (BlockHeader + data == PAGESIZE 바이트)
 */
static int flush_block_to_paged_file(const Block *b, int out_fd) {
    ssize_t n = write(out_fd, b, sizeof(*b));
    if (n != (ssize_t)sizeof(*b)) {
        perror("write block");
        return -1;
    }
    return 0;
}

/*
 * 비신장 가변길이 블로킹 + read() 기반 splitter (A안)
 * - 입력 파일의 레코드들을 Block 단위로 채우고
 * - Block 하나당 PAGESIZE 바이트짜리 Block 구조체를
 *   하나의 paged 파일에 연속해서 기록
 * - 나중에는 block_id * sizeof(Block) 오프셋으로 random access 가능
 */
int split_by_pagesize(const char* fileName)
{
    int fd = open(fileName, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("fstat");
        close(fd);
        return 1;
    }
    size_t filesize = (size_t)st.st_size;

    // 출력 paged 파일 이름: "<basename>_paged_<PAGESIZE>.dat"
    char outpath[PATH_MAX];
    const char *base = get_basename(fileName);
    snprintf(outpath, sizeof(outpath),
             "%s_paged_%d.dat", base, PAGESIZE);

    int out_fd = open(outpath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd < 0) {
        perror("open paged file");
        close(fd);
        return 1;
    }

    // Block/Index 초기화
    Block blk;
    RecIndex ri;
    uint32_t blk_id = 0;
    block_init(&blk, blk_id);
    recindex_reset(&ri);

    size_t max_payload = sizeof(blk.data);

    // read 버퍼
    unsigned char buf[PAGESIZE];
    // 레코드 누적 버퍼
    char recbuf[PAGESIZE];
    size_t rec_len = 0;

    uint64_t total_records = 0;
    uint64_t total_blocks = 0;
    uint64_t total_payload_bytes = 0;

    double t0 = now_ns();

    for (;;) {
        ssize_t n = read(fd, buf, sizeof(buf));  // PAGESIZE 단위로 읽기
        if (n < 0) {
            perror("read");
            close(fd);
            close(out_fd);
            return 1;
        }
        if (n == 0) {
            // EOF
            break;
        }

        size_t pos = 0;
        while (pos < (size_t)n) {
            unsigned char c = buf[pos++];

            if (c == '\n') {
                // 한 레코드 종료
                size_t len = rec_len;
                if (len > 0 && recbuf[len - 1] == '\r') {
                    len--;
                }

                if (len > 0) {
                    if (len > max_payload) {
                        fprintf(stderr,
                            "Error: record (%zu B) exceeds page payload (%zu B)\n",
                            len, max_payload);
                        close(fd);
                        close(out_fd);
                        return 2;
                    }

                    // 현재 Block에 안 들어가면, 기존 Block을 paged 파일에 flush
                    if (!block_can_fit(&blk, len)) {
                        if (blk.header.record_count > 0) {
                            if (flush_block_to_paged_file(&blk, out_fd) < 0) {
                                close(fd);
                                close(out_fd);
                                return 1;
                            }
                            total_blocks++;
                        }
                        block_init(&blk, ++blk_id);
                        recindex_reset(&ri);
                    }

                    // Block에 레코드 추가
                    uint32_t off = (uint32_t)block_used(&blk);
                    memcpy(blk.data + off, recbuf, len);
                    blk.header.remaining_space -= (uint16_t)len;
                    blk.header.record_count += 1;
                    recindex_add(&ri, off, (uint16_t)len); // 현재는 메모리 내에서만 사용

                    total_records++;
                    total_payload_bytes += (uint64_t)len;
                }

                rec_len = 0;
            } else {
                // 레코드 내용 누적
                if (rec_len >= sizeof(recbuf)) {
                    fprintf(stderr,
                            "Error: temporary record buffer overflow (>%zu)\n",
                            sizeof(recbuf));
                    close(fd);
                    close(out_fd);
                    return 2;
                }
                recbuf[rec_len++] = (char)c;
            }
        }
    }

    // 마지막 줄이 '\n' 없이 끝났을 경우 처리
    if (rec_len > 0) {
        size_t len = rec_len;
        if (len > 0 && recbuf[len - 1] == '\r') {
            len--;
        }

        if (len > 0) {
            if (len > max_payload) {
                fprintf(stderr,
                    "Error: record (%zu B) exceeds page payload (%zu B)\n",
                    len, max_payload);
                close(fd);
                close(out_fd);
                return 2;
            }

            if (!block_can_fit(&blk, len)) {
                if (blk.header.record_count > 0) {
                    if (flush_block_to_paged_file(&blk, out_fd) < 0) {
                        close(fd);
                        close(out_fd);
                        return 1;
                    }
                    total_blocks++;
                }
                block_init(&blk, ++blk_id);
                recindex_reset(&ri);
            }

            uint32_t off = (uint32_t)block_used(&blk);
            memcpy(blk.data + off, recbuf, len);
            blk.header.remaining_space -= (uint16_t)len;
            blk.header.record_count += 1;
            recindex_add(&ri, off, (uint16_t)len);

            total_records++;
            total_payload_bytes += (uint64_t)len;
        }

        rec_len = 0;
    }

    // 마지막 Block에 레코드가 남아 있으면 paged 파일에 flush
    if (blk.header.record_count > 0) {
        if (flush_block_to_paged_file(&blk, out_fd) < 0) {
            close(fd);
            close(out_fd);
            return 1;
        }
        total_blocks++;
    }

    double t1 = now_ns();
    double elapsed_s = (t1 - t0) / 1e9;

    fprintf(stderr,
            "\n=== Split Summary (PAGESIZE = %d, paged file) ===\n"
            "Input file bytes    : %zu\n"
            "Payload bytes(sum)  : %llu\n"
            "Records             : %llu\n"
            "Blocks written      : %llu\n"
            "Paged file          : %s\n"
            "Elapsed             : %.3f ms\n",
            PAGESIZE,
            filesize,
            (unsigned long long)total_payload_bytes,
            (unsigned long long)total_records,
            (unsigned long long)total_blocks,
            outpath,
            elapsed_s * 1e3);

    close(fd);
    close(out_fd);
    return 0;
}
