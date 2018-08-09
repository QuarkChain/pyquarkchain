#!/usr/bin/python3


# An implementation of heap that is able to quickly remove any item in
# O(1) by assuming an item has an heapIndex field.


class Heap:

    def __init__(self, cmp):
        self.heap = []
        self.heapSize = 0
        self.cmp = cmp

    def __assert_item(self, item):
        assert(self.heap[item.heapIndex] == item)

    def __swap(self, item1, item2):
        self.__assert_item(item1)
        self.__assert_item(item2)
        self.heap[item1.heapIndex] = item2
        self.heap[item2.heapIndex] = item1
        tmp = item1.heapIndex
        item1.heapIndex = item2.heapIndex
        item2.heapIndex = tmp

    def __get_parent(self, item):
        assert(item.heapIndex != 0)
        return self.heap[(item.heapIndex - 1) // 2]

    def __get_left_child(self, item):
        index = item.heapIndex * 2 + 1
        if index >= self.heapSize:
            return None
        return self.heap[index]

    def __get_right_child(self, item):
        index = item.heapIndex * 2 + 2
        if index >= self.heapSize:
            return None
        return self.heap[index]

    def __sift_up(self, item):
        assert(self.heap[item.heapIndex] == item)
        while item.heapIndex != 0:
            parent = self.__get_parent(item)
            if self.cmp(parent, item) <= 0:
                return
            self.__swap(parent, item)

    def __sift_down(self, item):
        while True:
            leftChild = self.__get_left_child(item)
            if leftChild is None:
                return

            rightChild = self.__get_right_child(item)
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
        self.__sift_up(item)

    def pop(self, item):
        lastItem = self.heap[self.heapSize - 1]
        self.__swap(item, lastItem)
        self.heapSize -= 1
        self.heap[self.heapSize] = None
        if lastItem != item:
            # TODO: Optimize
            self.__sift_down(lastItem)
            self.__sift_up(lastItem)
        return item

    def pop_top(self):
        return self.pop(self.heap[0])

    def size(self):
        return self.heapSize

    def is_empty(self):
        return self.heapSize == 0

    def __str__(self):
        return str(self.heap[:self.heapSize])

    # self check, used for testing only
    def check_integrity(self):
        for i in range(self.heapSize):
            item = self.heap[i]
            if i != 0:
                parent = self.__get_parent(item)
                if self.cmp(item, parent) < 0:
                    return False
            leftChild = self.__get_left_child(item)
            if leftChild is not None and self.cmp(leftChild, item) < 0:
                return False
            rightChild = self.__get_right_child(item)
            if rightChild is not None and self.cmp(rightChild, item) < 0:
                return False
        return True
