To build `qkchash` executable and `libqkchash.so` run
```bash
make
```
By default it uses `g++` to compile the C++ code.
You may use a different binary by overriding the `CC` variable. For example to use `g++-7` run
```bash
make CC=g++-7
```
Read `Makefile ` for more details.

To test the python wrapper run
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
