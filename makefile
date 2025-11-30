CC     = gcc
CFLAGS = -g -Wall -O2
TARGET = main.out

OBJS = main.o memory.o table.o blockio.o join.o

# OpenMP 사용: make OMP=1
ifdef OMP
  CFLAGS  += -fopenmp
  LDFLAGS += -fopenmp
endif

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f *.o $(TARGET)
	rm -f *.dat
	rm -f *.txt
	rm -f left_part_*.tmp right_part_*.tmp
