#!/usr/bin/env python3

import heapq


class PriorityQueue:
    def __init__(self, size):
        assert 0 < size
        self.body = []
        self.size = size

    def peek(self):
        return self.body[0]

    def push(self, item):
        if len(self.body) < self.size:
            heapq.heappush(self.body, item)
        else:
            if self.peek() < item:
                heapq.heappushpop(self.body, item)

    def pop(self):
        return heapq.heappop(self.body)
