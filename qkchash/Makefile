CC=g++
CFLAGS=-Wall -O3 -std=c++17

all: qkchash libqkchash.so qkchash_llrb

qkchash: qkchash.cpp util.h
	$(CC) $(CFLAGS) -o qkchash qkchash.cpp

libqkchash.so: qkchash.cpp util.h
	$(CC) $(CFLAGS) -o libqkchash.so -shared -fPIC qkchash.cpp

qkchash_llrb: qkchash_llrb.cpp qkchash_llrb.h util.h
	$(CC) $(CFLAGS) -o qkchash_llrb qkchash_llrb.cpp

clean:
	rm -f qkchash libqkchash.so qkchash_llrb
