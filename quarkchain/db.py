#!/usr/bin/python3
import copy
import pathlib
import shutil

from rocksdict import Rdict, Options, DBCompressionType


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

        options = Options(raw_mode=True)
        options.create_if_missing(True)
        options.set_max_open_files(100000)  # ubuntu 16.04 max files descriptors 524288
        options.set_write_buffer_size(128 * 1024 * 1024)  # 128 MiB
        options.set_max_write_buffer_number(3)
        options.set_target_file_size_base(67108864)
        options.set_compression_type(DBCompressionType.snappy())
        self._db = Rdict(db_path, options)

    def _destroy(self):
        shutil.rmtree(self.db_path, ignore_errors=True)

    def get(self, key, default=None):
        key = key.encode() if not isinstance(key, bytes) else key
        value = self._db.get(key)
        return default if value is None else value

    def multi_get(self, keys):
        keys = [k.encode() if not isinstance(k, bytes) else k for k in keys]
        values = self._db.get(keys)  # returns a list of values
        return dict(zip(keys, values))

    def put(self, key, value):
        key = key.encode() if not isinstance(key, bytes) else key
        value = bytes(value) if isinstance(value, bytearray) else value
        return self._db.put(key, value)

    def delete(self, key):
        key = key.encode() if not isinstance(key, bytes) else key
        return self._db.delete(key)

    def remove(self, key):
        return self.delete(key)

    def __contains__(self, key):
        key = key.encode() if not isinstance(key, bytes) else key
        return self._db.get(key) is not None

    def range_iter(self, start, end):
        """ A generator yielding (key, value) for keys in [start, end) ordered by key in ascending order"""
        it = self._db.iter()
        it.seek(start)
        while it.valid():
            k = it.key()
            if k >= end:
                return
            yield k, it.value()
            it.next()

    def reversed_range_iter(self, start, end):
        """ A generator yielding (key, value) for keys in (end, start] ordered key in descending order"""
        it = self._db.iter()
        it.seek_for_prev(start)
        while it.valid():
            k = it.key()
            if k <= end:
                return
            yield k, it.value()
            it.prev()

    def close(self):
        self._db.close()

    def __deepcopy__(self, memo):
        raise NotImplementedError
        # LevelDB cannot be deep copied
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == "db":
                setattr(result, k, v)
                continue
            setattr(result, k, copy.deepcopy(v, memo))
        return result


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
