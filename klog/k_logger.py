# coding=utf-8

import logging
import sys
from threading import Lock
from .exceptions import KLogException

logger = None

lock = Lock()


class KLogger:
    def __init__(self, level=logging.DEBUG):
        self.level = 0
        self.set_level(level)
        self.logger = None
        self._debug = None
        self._info = None
        self._warning = None
        self._error = None

    def set_logger(self, external_logger):
        self.logger = external_logger

        self._debug = self.logger.debug
        self._info = self.logger.info
        self._error = self.logger.error

        if hasattr(self.logger, "warning"):
            self._warning = self.logger.warning
        else:
            self._warning = self.logger.warning

    def set_level(self, level):
        if level not in [
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL
        ]:
            raise KLogException("KLoggerException", "level error")
        self.level = level

    def debug(self, message, *args):
        if logging.DEBUG >= self.level:
            self._debug(message % args)

    def info(self, message, *args):
        if logging.INFO >= self.level:
            self._info(message % args)

    def warning(self, message, *args):
        if logging.WARNING >= self.level:
            self._warning(message % args)

    def error(self, message, *args):
        if logging.ERROR >= self.level:
            self._error(message % args)


def create_default_logger():
    lg = logging.getLogger("KLog")
    lg.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
    ch.setFormatter(formatter)
    lg.addHandler(ch)
    return lg


def create_k_logger():
    with lock:
        global logger
        if logger is None:
            logger = KLogger()
            logger.set_logger(create_default_logger())


create_k_logger()


