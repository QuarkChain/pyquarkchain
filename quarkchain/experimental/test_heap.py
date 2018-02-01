#!/usr/bin/python3

from quarkchain.experimental import heap
import random
import unittest



class TestItem:

    def __init__(self, value):
        self.value = value
        self.heapIndex = 0

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.__str__()


class TestHeap(unittest.TestCase):

    def testHeapSort(self):
        N = 100
        data = [TestItem(i) for i in range(N)]
        random.shuffle(data)
        h = heap.Heap(lambda x, y: x.value - y.value)
        for d in data:
            h.push(d)
            self.assertTrue(h.checkIntegrity())
        for i in range(N):
            self.assertEqual(h.popTop().value, i)
            self.assertTrue(h.checkIntegrity())
        self.assertTrue(h.isEmpty())

    def testHeapRandomPop(self):
        N = 100
        data = [TestItem(i) for i in range(N)]
        random.shuffle(data)
        h = heap.Heap(lambda x, y: x.value - y.value)
        for d in data:
            h.push(d)
            self.assertTrue(h.checkIntegrity())
        random.shuffle(data)
        for d in data:
            h.pop(d)
            self.assertTrue(h.checkIntegrity())
        self.assertTrue(h.isEmpty())
