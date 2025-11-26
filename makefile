CC     = gcc
CFLAGS = -O3 -Wall -march=native -flto
TARGET = main.out

OBJS = main.o memory.o table.o blockio.o join.o

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f *.o $(TARGET)
	rm -f *.dat
	rm -f *.txt