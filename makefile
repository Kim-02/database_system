CC = gcc
CFLAGS = -g -Wall
OBJS = main.o
TARGET = main.out

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

main.o: main.c

clean:
	rm -f *.o $(TARGET)
	rm -f *.dat
	rm -f *.txt