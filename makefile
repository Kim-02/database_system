CC      = gcc
TARGET  = main.out

# 기본 최적화/경고
CFLAGS  = -Wall -Wextra -O3 -DNDEBUG -D_FILE_OFFSET_BITS=64 -march=native
LDFLAGS =

# 디버그 빌드: make DEBUG=1
ifdef DEBUG
  CFLAGS = -g -O0 -Wall -Wextra -D_FILE_OFFSET_BITS=64
endif

# OpenMP 사용: make OMP=1
ifdef OMP
  CFLAGS  += -fopenmp
  LDFLAGS += -fopenmp
endif

# LTO 사용: make LTO=1  (툴체인 지원 시)
ifdef LTO
  CFLAGS  += -flto
  LDFLAGS += -flto
endif

# 소스/오브젝트
SRCS = main.c memory.c table.c blockio.c join.c
OBJS = $(SRCS:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)
	rm -f *.dat *.txt
	rm -f left_part_*.tmp right_part_*.tmp out_part_*.tmp

.PHONY: all clean
