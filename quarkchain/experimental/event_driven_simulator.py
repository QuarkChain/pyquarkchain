#!/usr/bin/python3

import enum
import heap


class Task:
    class State(enum.Enum):
        SCHEDULED = 0
        FINISHED = 1
        CANCELLED = 2

    def __init__(self, scheduler, ts, callback, data):
        self.heap_index = 0
        self.scheduler = scheduler
        self.ts = ts
        self.callback = callback
        self.data = data
        self.state = Task.State.SCHEDULED

    def cancel(self):
        assert self.state == Task.State.SCHEDULED
        self.scheduler.cancel(self)

    def run(self):
        assert self.state == Task.State.SCHEDULED
        self.callback(self.ts, self.data)
        self.state = Task.State.FINISHED


class Scheduler:
    def __init__(self):
        self.ts = 0
        self.pq = heap.Heap(lambda task1, task2: task1.ts - task2.ts)
        self.terminated = False
        self.stopped = False

    def schedule_after(self, duration, callback, data):
        if self.stopped:
            return None
        task = Task(self, self.ts + duration, callback, data)
        self.pq.push(task)
        return task

    def cancel(self, task):
        self.pq.pop(task)

    def terminate(self):
        """ Terminate the scheduler immediately
        """
        self.terminated = True

    def stop(self):
        """ Stop the scheduler.  schedule_after() will return None thereafter.
        """
        self.stopped = True

    def loop_until_no_task(self):
        while not self.pq.is_empty() and not self.terminated:
            task = self.pq.pop_top()
            assert task.ts >= self.ts
            self.ts = task.ts
            task.run()
