# coding=utf-8

try:
    # python 3
    import queue
except ImportError:
    # python 2
    import Queue as queue

from .exceptions import KLogException


class QueueItem:
    def __init__(self, project_name, log_pool_name, data, timestamp):
        self.project_name = project_name
        self.log_pool_name = log_pool_name
        self.data = data
        self.timestamp = timestamp


class KQueue:
    MAX_SIZE = 100000
    MIN_SIZE = 1

    def __init__(self, maxsize):
        if maxsize > self.MAX_SIZE or maxsize < self.MIN_SIZE:
            raise KLogException("KQueueException", "maxsize should be in [{}, {}]".format(self.MIN_SIZE, self.MAX_SIZE))

        self.queue = queue.Queue(maxsize)

    def put(self, project_name, log_pool_name, data, timestamp, block=True):
        item = QueueItem(project_name, log_pool_name, data, timestamp)

        try:
            self.queue.put(item, block)
        except queue.Full:
            return False
        else:
            return True

    def get(self, block, timeout):
        try:
            return self.queue.get(block, timeout)
        except queue.Empty:
            return None

    def size(self):
        return self.queue.qsize()

