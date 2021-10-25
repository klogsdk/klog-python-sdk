# coding=utf-8

from threading import Lock
from .exceptions import KLogException


class DownSampler:
    MINIMAL_RATE = 1e-8
    MAX_COUNT = 1 << 30

    def __init__(self, rate=1.0):
        if rate > 1 or rate < self.MINIMAL_RATE:
            raise KLogException("DownSamplerException", "rate should be in ({}, 1]".format(self.MINIMAL_RATE))
        self.rate = rate
        self.divider = 1 / self.rate
        self.counter = -1
        self.lock = Lock()

    def ok(self):
        if self.rate == 1:
            return True
        with self.lock:
            self.counter += 1
            if self.counter == self.MAX_COUNT:
                self.counter = 0
            return self.counter % self.divider < 1

