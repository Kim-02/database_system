#include <stdio.h>
#include <stdint.h>

#define PAGESIZ 4096 //페이지 크기

typedef struct{
    uint32_t block_id; //블록 번호
    uint16_t record_count; //블록 안에 레코드 갯수
    uint16_t free_offset; //data안에 기록된 끝 위치
} BlockHeader;

typedef struct{
    BlockHeader header;
    unsigned char data[PAGESIZ - sizeof(BlockHeader)]; //헤더를 제외하고 기록가능한 레코드 범위
}Block;

