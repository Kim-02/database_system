CC = gcc
CFLAGS = -g -Wall
OBJS = main.o
TARGET = main.out

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS)

main.o: main.c blockdiv.h

clean:
	rm -f *.o $(TARGET)
