# coding=utf-8
from collections import OrderedDict
import logging
import time
import unittest
from klog.down_sampler import DownSampler
from klog.k_queue import KQueue
from klog.exceptions import KLogException
from klog.k_logger import logger
from klog.rate_limit import RateLimit
from klog.compress import Lz4Compressor
from klog.protobuf.klog_pb2 import LogGroup, Log
from klog.converters import convert_to_pb_log
from klog.credential import StaticCredential
from klog import Client


TestCase = unittest.TestCase


class TestDownSampler(TestCase):
    @staticmethod
    def count(n, rate):
        d = DownSampler(rate)
        count = 0
        for i in range(n):
            if d.ok():
                count += 1
        return count

    def test_count(self):
        self.assertEqual(1, self.count(1, 0.001))
        self.assertEqual(342, self.count(98765, 0.0034567))
        self.assertEqual(777, self.count(1234, 0.63))

    def test_param(self):
        with self.assertRaises(KLogException):
            DownSampler(-1)
        with self.assertRaises(KLogException):
            DownSampler(1.1)
        with self.assertRaises(KLogException):
            DownSampler(0)


class TestKQueue(TestCase):
    def test_param(self):
        with self.assertRaises(KLogException):
            KQueue(KQueue.MAX_SIZE + 1)
        with self.assertRaises(KLogException):
            KQueue(KQueue.MIN_SIZE - 1)
        KQueue(KQueue.MIN_SIZE)
        KQueue(KQueue.MAX_SIZE)

    def test_empty(self):
        queue = KQueue(4)
        self.assertEqual(0, queue.size())
        queue.put("test", "test", "a", time.time(), block=True)
        queue.put("test", "test", "a", time.time(), block=True)
        self.assertEqual(2, queue.size())

    def test_full(self):
        queue = KQueue(1)
        queue.put("test", "test", "a", time.time(), block=False)
        self.assertEqual(False, queue.put("test", "test", "a", time.time(), block=False))


class TestKLogger(TestCase):
    def setUp(self):
        class MockLogger:
            def __init__(self):
                self.output = ""

            def echo(self, message):
                self.output = message

            debug = echo
            info = echo
            warning = echo
            error = echo

        self.mock_external_logger = MockLogger()

    def test_logger(self):
        mock = self.mock_external_logger
        logger.set_level(logging.INFO)
        logger.set_logger(mock)

        logger.debug("Too old %s", "man")
        self.assertEqual("", mock.output)

        logger.info("%s Haha %s", "a", "b")
        self.assertEqual("a Haha b", mock.output)
        logger.warning("%s Haha %s", "a", "c")
        self.assertEqual("a Haha c", mock.output)
        logger.error("%s Haha %s", "a", "d")
        self.assertEqual("a Haha d", mock.output)

    def test_none_ascii(self):
        logger.info("%s", u"哈")


class TestRateLimit(TestCase):
    @staticmethod
    def duration(rl, total):
        t1 = time.time()
        for i in range(total):
            rl.wait()
        return time.time() - t1

    def test_create(self):
        RateLimit(limit_per_sec=100)
        RateLimit(limit_per_sec=1)
        with self.assertRaises(KLogException):
            RateLimit(limit_per_sec=0)
        with self.assertRaises(KLogException):
            RateLimit(limit_per_sec=222.1)
        with self.assertRaises(KLogException):
            RateLimit(limit_per_sec=10, slots_per_sec=100.1)
        with self.assertRaises(KLogException):
            RateLimit(limit_per_sec=10, slots_per_sec=-1)

    def test_limit(self):
        duration = self.duration(RateLimit(limit_per_sec=1), total=3)
        self.assertTrue(3.5 > duration > 2.5)

        duration = self.duration(RateLimit(limit_per_sec=543), total=543*3)
        self.assertTrue(3.5 > duration > 2.5)


class TestProtobuf(TestCase):
    def test_pb_log(self):
        pb_log = Log()
        content = Log.Content()
        content.key = "message"
        content.value = "ha ha"
        pb_log.contents.append(content)

        lg = LogGroup()
        lg.logs.append(pb_log)

    def test_convert_string(self):
        timestamp = time.time()
        pb_log1 = convert_to_pb_log("ha ha", timestamp)

        pb_log2 = Log()
        pb_log2.time = int(timestamp * 1000)
        content = Log.Content()
        content.key = "message"
        content.value = "ha ha"
        pb_log2.contents.append(content)

        self.assertEqual(pb_log1.ByteSize(), pb_log2.ByteSize())

    def test_convert_bool(self):
        timestamp = time.time()
        pb_log1 = convert_to_pb_log(True, timestamp)

        pb_log2 = Log()
        pb_log2.time = int(timestamp * 1000)
        content = Log.Content()
        content.key = "message"
        content.value = "True"
        pb_log2.contents.append(content)

        self.assertEqual(pb_log1.ByteSize(), pb_log2.ByteSize())

    def test_convert_dict(self):
        timestamp = time.time()

        dic = OrderedDict()
        dic["a"] = "v"
        dic["b"] = 2.0
        dic["sub_dic"] = {"s_a": 3}

        pb_log1 = convert_to_pb_log(dic, timestamp)

        pb_log2 = Log()
        pb_log2.time = int(timestamp * 1000)

        content = Log.Content()
        content.key = "a"
        content.value = "v"
        pb_log2.contents.append(content)

        content = Log.Content()
        content.key = "b"
        content.value = "2.0"
        pb_log2.contents.append(content)

        content = Log.Content()
        content.key = "sub_dic"
        content.value = '{"s_a": 3}'
        pb_log2.contents.append(content)

        self.assertEqual(pb_log1.ByteSize(), pb_log2.ByteSize())


class TestLz4Compress(TestCase):
    def setUp(self):
        self.c = Lz4Compressor()

    def test_compress_small(self):
        raw = "ha ha".encode("ascii")
        lz4 = self.c.compress(raw)
        self.assertEqual(raw, self.c.decompress(lz4))

    def test_compress_large(self):
        raw = ("ha ha, hou" * 200000).encode("ascii")
        lz4 = self.c.compress(raw)
        self.assertEqual(raw, self.c.decompress(lz4))

    def test_compress_pb_small(self):
        pb_log = convert_to_pb_log("ha ha, hou", time.time())
        lg = LogGroup()
        lg.logs.append(pb_log)
        raw1 = lg.SerializeToString()

        lz4 = self.c.compress(raw1)
        raw2 = self.c.decompress(lz4)
        self.assertEqual(raw1, raw2)

        lg2 = LogGroup()
        lg2.ParseFromString(raw2)
        self.assertEqual(lg2.SerializeToString(), raw1)

    def test_compress_pb_large(self):
        lg = LogGroup()
        for i in range(5000):
            dic = {
                "_timestamp_": 1632215713983,
                "request_time": "0.003",
                "content_type": "-",
                "http_cookie": "-",
                "remote_addr_port": "222.222.222.222:55555",
                "timestamp": 1632215714008,
                "log_pool_name": "log_poool_1234567890",
                "request_uri": "/29j3f00k?s0f0=jowijef&oijwjef=aoejfj&oasf=isuf",
                "server_real_addr_v6": "-",
                "request_length": "340",
                "server_real_addr_port": "222.111.33.44:2222",
                "user_id": "99999999",
                "http_referer": "-",
                "http_x_forwarded_for": "-",
                "upstream_response_time": "0.001",
                "server_protocol": "HTTP/1.1",
                "status": "502",
                "server_name": "asdfasb.erberf.net",
                "scheme": "https",
                "upstream_addr": "192.168.1.1:8888",
                "body_bytes_sent": "327",
                "ha_id": "f3f3f3-334g34-45b45-45h45-45h45h",
                "request_method": "GET",
                "http_host": "asdv3f3.asdfv3.net:8888",
                "http_user_agent": "ios/76423 CFNetwork/1240.0.4 Darwin/20.6.0",
                "time_iso8601": "2021-09-21T17:15:11+08:00",
                "upstream_status": "502",
            }
            pb_log = convert_to_pb_log(dic, time.time())
            lg.logs.append(pb_log)

        raw1 = lg.SerializeToString()

        lz4 = self.c.compress(raw1)
        raw2 = self.c.decompress(lz4)

        lg2 = LogGroup()
        lg2.ParseFromString(raw2)

        self.assertEqual(raw1, raw2)
        self.assertEqual(lg2.SerializeToString(), raw1)


class TestCredential(TestCase):
    def test_credential(self):
        c = StaticCredential("ak", "sk")
        self.assertEqual("ak", c.get_access_key())
        self.assertEqual("sk", c.get_secret_key())

    def test_client(self):
        with self.assertRaises(KLogException):
            Client("endpoint")
        with self.assertRaises(KLogException):
            Client("endpoint", access_key="ak", secret_key="")
        with self.assertRaises(KLogException):
            Client("endpoint", access_key="", secret_key="sk")
        with self.assertRaises(KLogException):
            Client("endpoint", access_key="", secret_key="", credential=StaticCredential("ak", ""))
        with self.assertRaises(KLogException):
            Client("endpoint", access_key="", secret_key="", credential=StaticCredential("", "sk"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
