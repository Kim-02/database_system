CC = gcc
CFLAGS = -g -Wall
OBJS = main.o
TARGET = main.out

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

main.o: main.c table_split.h

clean:
	rm -f *.o $(TARGET)
