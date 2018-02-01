#!/usr/bin/python3

import enum
import heap


class Task:

    class State(enum.Enum):
        SCHEDULED = 0
        FINISHED = 1
        CANCELLED = 2

    def __init__(self, scheduler, ts, callback, data):
        self.heapIndex = 0
        self.scheduler = scheduler
        self.ts = ts
        self.callback = callback
        self.data = data
        self.state = Task.State.SCHEDULED

    def cancel(self):
        assert(self.state == Task.State.SCHEDULED)
        self.scheduler.cancel(self)

    def run(self):
        assert(self.state == Task.State.SCHEDULED)
        self.callback(self.ts, self.data)
        self.state = Task.State.FINISHED


class Scheduler:

    def __init__(self):
        self.ts = 0
        self.pq = heap.Heap(lambda task1, task2: task1.ts - task2.ts)

    def scheduleAfter(self, duration, callback, data):
        task = Task(self, self.ts + duration, callback, data)
        self.pq.push(task)
        return task

    def cancel(self, task):
        self.pq.pop(task)

    def loopUntilNoTask(self):
        while not self.pq.isEmpty():
            task = self.pq.popTop()
            assert(task.ts >= self.ts)
            self.ts = task.ts
            task.run()
