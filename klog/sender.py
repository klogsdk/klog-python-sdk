# coding=utf-8

import threading
import time

from .auth import KAuth
from .compress import Lz4Compressor
from .const import X_KLOG_API_VERSION, \
    MAX_KEY_COUNT, MAX_KEY_SIZE, MAX_VALUE_SIZE, \
    MAX_LOG_SIZE, MAX_LOG_GROUP_SIZE, MAX_BULK_SIZE, MAX_RETRIES, \
    ERR_MAX_KEY_COUNT, ERR_MAX_KEY_SIZE, ERR_MAX_VALUE_SIZE, ERR_MAX_LOG_SIZE
from .converters import convert_to_pb_log
from .k_http import KHttp
from .k_logger import logger
from .protobuf.klog_pb2 import LogGroup


class Buffer:
    def __init__(self):
        self.size = 0
        self.log_group = LogGroup()

    def add(self, pb_log, size):
        self.size += size
        self.log_group.logs.append(pb_log)


class Sender:
    http_client = None
    locker = threading.RLock()

    def __init__(self,
                 endpoint,
                 credential,
                 project_name,
                 log_pool_name,
                 max_retries,
                 retry_interval):

        self.auth = KAuth(credential)
        self.project_name = project_name
        self.log_pool_name = log_pool_name

        self.endpoint = endpoint
        self.max_retries = max_retries if max_retries >= 0 else MAX_RETRIES
        self.retry_interval = retry_interval

        self.compressor = Lz4Compressor()
        self.buffers = []
        self.last_send_time = time.time()
        self.method = "POST"
        self.api = "/PutLogs"
        self.query_string = "ProjectName={}&LogPoolName={}".format(self.project_name, self.log_pool_name)

        self.create_client()

    def create_client(self):
        with self.locker:
            if self.http_client is None:
                self.http_client = KHttp(self.endpoint)

    def add_data(self, item):
        try:
            pb_log = convert_to_pb_log(item.data, item.timestamp)
        except Exception as e:
            logger.warning("KLog.Sender.add_data: 1 log dropped while converting protobuf, error=%s", e)
            return

        err = self.check(pb_log)
        if err:
            logger.warning("KLog.Sender.add_data: 1 log dropped, error=%s", err)
            return

        # 每条 Log 的大小再加 2，累加起来恰好等于 LogGroup 的大小。
        size = pb_log.ByteSize() + 2

        if size > MAX_LOG_SIZE:
            logger.warning("KLog.Sender.add_data: 1 log dropped, error=%s", ERR_MAX_LOG_SIZE)
            return

        if len(self.buffers) == 0:
            self.buffers.append(Buffer())

        buf = self.buffers[0]
        if buf.size + size > MAX_LOG_GROUP_SIZE or len(buf.log_group.logs) >= MAX_BULK_SIZE:
            buf = Buffer()
            self.buffers.append(buf)

        buf.add(pb_log, size)
        return

    def send(self):
        if not self.has_buffer():
            return True

        lg = self.buffers[0].log_group
        logger.debug("KLog.Sender.send: processing %s logs.", len(lg.logs))

        data = lg.SerializeToString()

        klog_headers = {
            "X-Klog-Api-Version": X_KLOG_API_VERSION,
            "X-Klog-Signature-Method": "hmac-sha1",
        }

        if self.compressor:
            data = self.compressor.compress(data)
            klog_headers["X-Klog-Compress-Type"] = self.compressor.name()

        retried = 0
        sleep_sec = 1
        url = "{}?{}".format(self.api, self.query_string)
        while True:
            headers = self.auth.get_headers(self.method, self.api, self.query_string,
                                            data, "application/x-protobuf", klog_headers)

            status_code, response_body, exception = \
                self.http_client.do_request(self.method, url, data=data, headers=headers)

            # 成功
            if status_code == 200:
                break

            logger.warning("KLog.Sender.send: %s url=%s%s, status=%s, exception=%s, response_body=%s",
                           self.method, self.endpoint, self.api, status_code, exception, response_body)

            # 设置了最大重试次数，如果达到次数，停止重试
            if retried >= self.max_retries >= 0:
                break

            if self.retry_interval > 0:
                # 设置了重试间隔
                time.sleep(self.retry_interval)
            else:
                # 未设置重试间隔，将依次增加重试时间，最长1分钟
                sleep_sec = sleep_sec * 2 if sleep_sec < 60 else 60
                time.sleep(sleep_sec)

            retried += 1

        # 失败，日志丢弃
        if status_code != 200:
            logger.error("KLog.Sender.send: max retries(%s times) reached, %s log drops.",
                         self.max_retries, len(lg.logs))

        self.buffers = self.buffers[1:]
        self.last_send_time = time.time()
        return status_code == 200

    def buffer_full(self):
        return len(self.buffers) > 1

    def has_buffer(self):
        return len(self.buffers) > 0

    def get_last_send_time(self):
        return self.last_send_time

    @staticmethod
    def check(pb_log):
        if len(pb_log.contents) > MAX_KEY_COUNT:
            return ERR_MAX_KEY_COUNT
        for kv in pb_log.contents:
            if len(kv.key) > MAX_KEY_SIZE:
                return ERR_MAX_KEY_SIZE
            elif len(kv.value) > MAX_VALUE_SIZE:
                return ERR_MAX_VALUE_SIZE
