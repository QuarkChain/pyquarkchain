#!/usr/bin/python3


# An implementation of heap that is able to quickly remove any item in
# O(1) by assuming an item has an heap_index field.


class Heap:
    def __init__(self, cmp):
        self.heap = []
        self.heap_size = 0
        self.cmp = cmp

    def __assert_item(self, item):
        assert self.heap[item.heap_index] == item

    def __swap(self, item1, item2):
        self.__assert_item(item1)
        self.__assert_item(item2)
        self.heap[item1.heap_index] = item2
        self.heap[item2.heap_index] = item1
        tmp = item1.heap_index
        item1.heap_index = item2.heap_index
        item2.heap_index = tmp

    def __get_parent(self, item):
        assert item.heap_index != 0
        return self.heap[(item.heap_index - 1) // 2]

    def __get_left_child(self, item):
        index = item.heap_index * 2 + 1
        if index >= self.heap_size:
            return None
        return self.heap[index]

    def __get_right_child(self, item):
        index = item.heap_index * 2 + 2
        if index >= self.heap_size:
            return None
        return self.heap[index]

    def __sift_up(self, item):
        assert self.heap[item.heap_index] == item
        while item.heap_index != 0:
            parent = self.__get_parent(item)
            if self.cmp(parent, item) <= 0:
                return
            self.__swap(parent, item)

    def __sift_down(self, item):
        while True:
            left_child = self.__get_left_child(item)
            if left_child is None:
                return

            right_child = self.__get_right_child(item)
            min_child = left_child
            if right_child is not None and self.cmp(right_child, left_child) < 0:
                min_child = right_child

            if self.cmp(min_child, item) < 0:
                self.__swap(item, min_child)
            else:
                break

    def push(self, item):
        if self.heap_size == len(self.heap):
            self.heap.append(item)
        else:
            self.heap[self.heap_size] = item
        item.heap_index = self.heap_size
        self.heap_size += 1
        self.__sift_up(item)

    def pop(self, item):
        last_item = self.heap[self.heap_size - 1]
        self.__swap(item, last_item)
        self.heap_size -= 1
        self.heap[self.heap_size] = None
        if last_item != item:
            # TODO: Optimize
            self.__sift_down(last_item)
            self.__sift_up(last_item)
        return item

    def pop_top(self):
        return self.pop(self.heap[0])

    def size(self):
        return self.heap_size

    def is_empty(self):
        return self.heap_size == 0

    def __str__(self):
        return str(self.heap[: self.heap_size])

    # self check, used for testing only
    def check_integrity(self):
        for i in range(self.heap_size):
            item = self.heap[i]
            if i != 0:
                parent = self.__get_parent(item)
                if self.cmp(item, parent) < 0:
                    return False
            left_child = self.__get_left_child(item)
            if left_child is not None and self.cmp(left_child, item) < 0:
                return False
            right_child = self.__get_right_child(item)
            if right_child is not None and self.cmp(right_child, item) < 0:
                return False
        return True
