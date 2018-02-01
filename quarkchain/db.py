#!/usr/bin/python3


class InMemoryDb:
    """ A simple in-memory key-value database
    """

    def __init__(self):
        self.kv = dict()

    def get(self, key, default=None):
        return self.kv.get(key, default)

    def set(self, key, value):
        self.kv[key] = value

    def remove(self, key):
        del self.kv[key]

    def __contains__(self, key):
        return key in self.kv


DB = InMemoryDb()
