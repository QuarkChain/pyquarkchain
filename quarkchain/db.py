#!/usr/bin/python3
import copy
import pathlib
import shutil

from rocksdict import Rdict


class Db:
    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError("cannot find {}".format(key))
        return value

    def close(self):
        pass


class InMemoryDb(Db):
    """ A simple in-memory key-value database
    TODO: Need to support range operation (e.g., leveldb)
    """

    def __init__(self):
        self.kv = dict()

    def range_iter(self, start, end):
        keys = []
        for k in self.kv.keys():
            if k >= start and k < end:
                keys.append(k)
        keys.sort()
        for k in keys:
            yield k, self.kv[k]

    def reversed_range_iter(self, start, end):
        keys = []
        for k in self.kv.keys():
            if k <= start and k > end:
                keys.append(k)
        keys.sort(reverse=True)
        for k in keys:
            yield k, self.kv[k]

    def get(self, key, default=None):
        return self.kv.get(key, default)

    def put(self, key, value):
        self.kv[key] = bytes(value)

    def remove(self, key):
        del self.kv[key]

    def __contains__(self, key):
        return key in self.kv


class PersistentDb(Db):
    def __init__(self, db_path, clean=False):
        self.db_path = db_path
        if clean:
            self._destroy()
        pathlib.Path(self.db_path).mkdir(parents=True, exist_ok=True)

        self._db = Rdict(self.db_path)

    def _destroy(self):
        shutil.rmtree(self.db_path, ignore_errors=True)

    def get(self, key, default=None):
        key = key.encode() if not isinstance(key, bytes) else key
        value = self._db[key]
        return default if value is None else value

    def put(self, key, value):
        key = key.encode() if not isinstance(key, bytes) else key
        value = bytes(value) if isinstance(value, bytearray) else value
        self._db[key] = value

    def delete(self, key):
        key = key.encode() if not isinstance(key, bytes) else key
        try:
            del self._db[key]
        except KeyError:
            pass

    def remove(self, key):
        return self.delete(key)

    def __contains__(self, key):
        key = key.encode() if not isinstance(key, bytes) else key
        return self._db[key] is not None

    def range_iter(self, start, end):
        """ A generator yielding (key, value) for keys in [start, end) ordered by key in ascending order"""
        for k, v in self._db.iter(start, end):
            yield k, v

    def reversed_range_iter(self, start, end):
        """ A generator yielding (key, value) for keys in (end, start] ordered key in descending order"""
        for k, v in reversed(list(self._db.iter(end, start))):
            yield k, v

    def close(self):
        # No close() available for rocksdb
        # see https://github.com/twmht/python-rocksdb/issues/10
        self._db.close()

    def __deepcopy__(self, memo):
        raise NotImplementedError


class OverlayDb(Db):
    """ Used for making temporary objects in EvmState.ephemeral_clone()"""

    def __init__(self, db):
        self._db = db
        self.kv = None
        self.overlay = {}

    def get(self, key):
        if key in self.overlay:
            return self.overlay[key]
        return self._db.get(key)

    def put(self, key, value):
        self.overlay[key] = value

    def delete(self, key):
        self.overlay[key] = None

    def commit(self):
        pass

    def _has_key(self, key):
        if key in self.overlay:
            return self.overlay[key] is not None
        return self._db.get(key) is not None

    def __contains__(self, key):
        return self._has_key(key)
