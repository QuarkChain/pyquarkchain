CC=g++
CFLAGS=-Wall -O3 -std=c++17

all: qkchash libqkchash.so

qkchash: qkchash.cpp
	$(CC) $(CFLAGS) -o qkchash qkchash.cpp

libqkchash.so: qkchash.cpp
	$(CC) $(CFLAGS) -o libqkchash.so -shared -fPIC qkchash.cpp

clean:
	rm -f qkchash libqkchash.so
