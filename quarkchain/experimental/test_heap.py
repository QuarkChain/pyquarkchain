#!/usr/bin/python3

from quarkchain.experimental import heap
import random
import unittest


class HeapTestItem:
    def __init__(self, value):
        self.value = value
        self.heap_index = 0

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.__str__()


class TestHeap(unittest.TestCase):
    def test_heap_sort(self):
        N = 100
        data = [HeapTestItem(i) for i in range(N)]
        random.shuffle(data)
        h = heap.Heap(lambda x, y: x.value - y.value)
        for d in data:
            h.push(d)
            self.assertTrue(h.check_integrity())
        for i in range(N):
            self.assertEqual(h.pop_top().value, i)
            self.assertTrue(h.check_integrity())
        self.assertTrue(h.is_empty())

    def test_heap_random_pop(self):
        N = 100
        data = [HeapTestItem(i) for i in range(N)]
        random.shuffle(data)
        h = heap.Heap(lambda x, y: x.value - y.value)
        for d in data:
            h.push(d)
            self.assertTrue(h.check_integrity())
        random.shuffle(data)
        for d in data:
            h.pop(d)
            self.assertTrue(h.check_integrity())
        self.assertTrue(h.is_empty())
