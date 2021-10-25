# coding=utf-8

from datetime import datetime
import json
import threading
import time

from .const import MAX_RETRIES, MAX_BULK_SIZE
from .k_logger import logger

try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None


class Buffer:
    def __init__(self):
        self.size = 0
        self.list = list()

    def add(self, string_data, size):
        self.size += size
        self.list.append(string_data)


class EsSender:
    MAX_LOG_GROUP_SIZE = 5 << 20
    locker = threading.RLock()
    client = None

    def __init__(self,
                 endpoints,
                 max_retries,
                 retry_interval,
                 **kwargs):

        self.kwargs = kwargs

        self.endpoints = endpoints
        self.max_retries = max_retries if max_retries >= 0 else MAX_RETRIES
        self.retry_interval = retry_interval

        self.buffers = []
        self.last_send_time = time.time()

        self.create_client()

    def create_client(self):
        with self.locker:
            if self.client is None and Elasticsearch is not None:
                self.client = Elasticsearch(self.endpoints, **self.kwargs)

    def add_data(self, item):
        doc = self.convert_to_doc(item.data, item.timestamp)
        try:
            data = json.dumps(doc)
        except Exception as e:
            logger.warning("KLog.EsSender.add_data: 1 log dropped while json dumps, error=%s", e)
            return

        index = datetime.now().strftime(item.log_pool_name)
        action = '{{"index":{{"_index": "{}"}}}}'.format(index)
        action_size = len(action)
        data_size = len(data)

        if len(self.buffers) == 0:
            self.buffers.append(Buffer())

        buf = self.buffers[0]
        if buf.size + action_size + data_size > self.MAX_LOG_GROUP_SIZE or len(buf.list) / 2 >= MAX_BULK_SIZE:
            buf = Buffer()
            self.buffers.append(buf)

        buf.add(action, action_size)
        buf.add(data, data_size)
        return

    def send(self):
        if not self.has_buffer() or self.client is None:
            return True
        
        lines = self.buffers[0].list
        logger.debug("KLog.EsSender.send: processing %s logs.", int(len(lines) / 2))

        retried = 0
        sleep_sec = 1
        while True:
            error_type = ""
            error_reason = ""
            retry_lines = []

            try:
                resp = self.client.bulk(body="\n".join(lines))
            except Exception as e:
                error_type = "bulkException"
                error_reason = str(e)
            else:
                logger.debug("KLog.EsSender.send: bulk hasErrors=%s", resp.get("errors"))
                for i in range(int(len(lines) / 2)):
                    info = resp["items"][i]["index"]
                    status_code = info.get("status", 500)
                    ok = 200 <= status_code < 300
                    if not ok:
                        error = info.get("error", dict())
                        error_type = error.get("type", "")
                        error_reason = error.get("reason", "")
                        if status_code == 400:
                            # 抛弃，打印日志
                            logger.warning("KLog.EsSender.send: "
                                           "1 log dropped because of error_type=%s, error_reason=%s",
                                           error_type, error_reason)
                        else:
                            retry_lines.append(lines[2*i])
                            retry_lines.append(lines[2*i + 1])
            # 完成
            if len(retry_lines) == 0:
                break

            logger.warning("KLog.EsSender.send: got error, error_type=%s, error_reason=%s",
                           error_type, error_reason)

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
            lines = retry_lines

        # 失败，日志丢弃
        if retry_lines:
            logger.error("KLog.EsSender.send: max retries(%s times) reached, stop retry", self.max_retries)

        self.buffers = self.buffers[1:]
        self.last_send_time = time.time()
        return not bool(lines)

    def buffer_full(self):
        return len(self.buffers) > 1

    def has_buffer(self):
        return len(self.buffers) > 0

    def get_last_send_time(self):
        return self.last_send_time

    @staticmethod
    def convert_to_doc(data, timestamp):
        if isinstance(data, dict):
            data["timestamp"] = timestamp
            return data
        else:
            return {
                "message": data,
                "timestamp": timestamp,
            }
