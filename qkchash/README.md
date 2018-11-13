To compile the library on Linux, need to run:
```bash
g++ -shared -o libqkchash.so -fPIC qkchash.cpp -O3 -std=gnu++17
```

And run:
```
LD_LIBRARY_PATH=. python3 qkchash.py
```

The output is:
```
make_cache time: 0.09
Python version, time used: 0.46, hashes per sec: 21.54
Native version, time used: 6.16, hashes per sec: 162.23
Equal: True
```
