#!/usr/bin/python3


# An implementation of heap that is able to quickly remove any item in
# O(1) by assuming an item has an heapIndex field.


class Heap:

    def __init__(self, cmp):
        self.heap = []
        self.heapSize = 0
        self.cmp = cmp

    def __assertItem(self, item):
        assert(self.heap[item.heapIndex] == item)

    def __swap(self, item1, item2):
        self.__assertItem(item1)
        self.__assertItem(item2)
        self.heap[item1.heapIndex] = item2
        self.heap[item2.heapIndex] = item1
        tmp = item1.heapIndex
        item1.heapIndex = item2.heapIndex
        item2.heapIndex = tmp

    def __getParent(self, item):
        assert(item.heapIndex != 0)
        return self.heap[(item.heapIndex - 1) // 2]

    def __getLeftChild(self, item):
        index = item.heapIndex * 2 + 1
        if index >= self.heapSize:
            return None
        return self.heap[index]

    def __getRightChild(self, item):
        index = item.heapIndex * 2 + 2
        if index >= self.heapSize:
            return None
        return self.heap[index]

    def __siftUp(self, item):
        assert(self.heap[item.heapIndex] == item)
        while item.heapIndex != 0:
            parent = self.__getParent(item)
            if self.cmp(parent, item) <= 0:
                return
            self.__swap(parent, item)

    def __siftDown(self, item):
        while True:
            leftChild = self.__getLeftChild(item)
            if leftChild is None:
                return

            rightChild = self.__getRightChild(item)
            minChild = leftChild
            if rightChild is not None and self.cmp(rightChild, leftChild) < 0:
                minChild = rightChild

            if self.cmp(minChild, item) < 0:
                self.__swap(item, minChild)
            else:
                break

    def push(self, item):
        if self.heapSize == len(self.heap):
            self.heap.append(item)
        else:
            self.heap[self.heapSize] = item
        item.heapIndex = self.heapSize
        self.heapSize += 1
        self.__siftUp(item)

    def pop(self, item):
        lastItem = self.heap[self.heapSize - 1]
        self.__swap(item, lastItem)
        self.heapSize -= 1
        self.heap[self.heapSize] = None
        if lastItem != item:
            # TODO: Optimize
            self.__siftDown(lastItem)
            self.__siftUp(lastItem)
        return item

    def popTop(self):
        return self.pop(self.heap[0])

    def size(self):
        return self.heapSize

    def is_empty(self):
        return self.heapSize == 0

    def __str__(self):
        return str(self.heap[:self.heapSize])

    # self check, used for testing only
    def checkIntegrity(self):
        for i in range(self.heapSize):
            item = self.heap[i]
            if i != 0:
                parent = self.__getParent(item)
                if self.cmp(item, parent) < 0:
                    return False
            leftChild = self.__getLeftChild(item)
            if leftChild is not None and self.cmp(leftChild, item) < 0:
                return False
            rightChild = self.__getRightChild(item)
            if rightChild is not None and self.cmp(rightChild, item) < 0:
                return False
        return True
