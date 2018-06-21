#import leveldb
#import plyvel
import os
import time
from quarkchain.db import PersistentDb

def test_plyvel(n):
    db = PersistentDb("./dbdb")
    for i in range(n):
        db.put(i.to_bytes(4, "big"), os.urandom(100))


def test_leveldb(n):
    db = leveldb.LevelDB("/tmp/def")
    for i in range(n):
        db.Put(i.to_bytes(4, "big"), os.urandom(100))


if __name__ == "__main__":
    s = time.time()
    test_plyvel(100000)
    print("plyvel {:.2f}".format(time.time() - s))
    s = time.time()
 #   test_leveldb(100000)
    print("leveldb {:.2f}".format(time.time() - s))

