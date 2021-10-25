# coding=utf-8


class KLogException(Exception):
    def __init__(self, name, reason):
        self.name = name
        self.reason = reason

    def __repr__(self):
        if self.reason:
            return "{}: {}".format(self.name, self.reason)
        else:
            return self.name

    __str__ = __repr__
