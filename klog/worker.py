# coding=utf-8
import time
import threading

from .es_sender import EsSender
from .exceptions import KLogException
from .k_logger import logger
from .rate_limit import RateLimit
from .sender import Sender


class Worker:
    def __init__(self, queue,
                 endpoint,
                 credential,
                 rate_limit,
                 max_retries,
                 retry_interval,
                 use_es="off",
                 **kwargs):

        self.queue = queue
        self.endpoint = endpoint
        self.credential = credential
        self.max_retries = max_retries
        self.retry_interval = retry_interval

        if rate_limit > 0:
            self.rate_limit = RateLimit(limit_per_sec=rate_limit)
        else:
            self.rate_limit = None

        self.senders = dict()

        self.flush_event = threading.Event()
        self.flush_done_event = threading.Event()

        self.use_es = use_es
        self.kwargs = kwargs
        self.check_env()

    def flush(self, timeout=10):
        logger.debug("Worker.flush: got flush signal, timeout=%s", timeout)
        self.flush_event.set()
        self.flush_done_event.wait(timeout)
        self.flush_event.clear()
        self.flush_done_event.clear()

    def run(self):
        last_checked_at = time.time()
        while True:
            item = self.queue.get(block=True, timeout=0.1)

            # buffer满，发送
            if item:
                if self.rate_limit:
                    self.rate_limit.wait()
                sender = self.get_sender(item.project_name, item.log_pool_name)
                sender.add_data(item)
                if sender.buffer_full():
                    sender.send()

            # 超过一定时间间隔，发送
            now = time.time()
            if now - last_checked_at > 0.1:
                last_checked_at = now
                for sender in self.senders.values():
                    if now - sender.get_last_send_time() > 2:
                        sender.send()

            # 收到flush信号，发送
            if self.flush_event.is_set() and self.queue.size() == 0:
                logger.debug("KLog.Worker.run: got flush signal and queue is empty")
                for sender in self.senders.values():
                    sender.send()
                self.flush_done_event.set()

    def get_sender(self, project_name, log_pool_name):
        if self.use_es == "on":
            # 使用 es sender
            if "es_sender" not in self.senders:
                self.senders["es_sender"] = EsSender(self.endpoint,
                                                     max_retries=self.max_retries,
                                                     retry_interval=self.retry_interval,
                                                     **self.kwargs)
            return self.senders["es_sender"]

        # 使用KLog sender
        key = "{}__{}".format(project_name, log_pool_name)
        sender = self.senders.get(key)
        if not sender:
            sender = Sender(self.endpoint,
                            self.credential,
                            project_name,
                            log_pool_name,
                            self.max_retries,
                            self.retry_interval)
            self.senders[key] = sender
        return sender

    def check_env(self):
        if self.use_es == "on":
            try:
                from elasticsearch import Elasticsearch
            except ImportError:
                raise KLogException("NoElasticsearchDriverException",
                                    "you are using elasticsearch as logging backend, please install elasticsearch-py")
        else:
            if self.credential is None or not self.credential.get_access_key() or not self.credential.get_secret_key():
                raise KLogException("NoCredentialException", "please set credential or access_key and secret_key")
