# coding=utf-8

import time
from .exceptions import KLogException


class RateLimit:
    def __init__(self, limit_per_sec, slots_per_sec=10):
        if type(slots_per_sec) != int or slots_per_sec < 0:
            raise KLogException("RateLimitException", "slots_per_sec must be integer and >= 1.")
        if type(limit_per_sec) != int or limit_per_sec < 1:
            raise KLogException("RateLimitException", "rate_limit should be integer and >= 1.")

        self.slots_len = slots_per_sec
        self.slots = [0] * self.slots_len

        self.limit_per_sec = limit_per_sec
        self.limit_per_slot_per_sec = float(self.limit_per_sec) / self.slots_len

        self.previous_i = 0
        self.previous_time = time.time()

    def wait(self):
        added = False
        while True:
            # 距离上次调用超过1秒，清零计数器
            now = time.time()
            if now > self.previous_time + 1:
                for i in range(self.slots_len):
                    self.slots[i] = 0

            # 每隔一段时间切换一次slot
            i = int(now * self.slots_len % self.slots_len)
            if self.previous_i != i:
                self.slots[i] = 0
                self.previous_i = i

            # 计数
            if not added:
                self.slots[i] += 1
                added = True

            # 每秒总数、每slot计数分别不得超限
            if sum(self.slots) >= self.limit_per_sec or self.slots[i] > self.limit_per_slot_per_sec > 0:
                # 如果超限，休眠一个slot的跨度
                time.sleep(1.0 / self.slots_len)
            else:
                break

        self.previous_time = now
