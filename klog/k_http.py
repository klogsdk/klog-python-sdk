# coding=utf-8

import time

try:
    # python 3
    import http.client as http
    from urllib.parse import urlparse
except ImportError:
    # python 2
    import httplib as http
    from urlparse import urlparse

from .k_logger import logger


class KHttp:
    def __init__(self, endpoint, timeout=10):
        self.endpoint = endpoint

        p = urlparse(endpoint)
        connection = http.HTTPSConnection if p.scheme == "https" else http.HTTPConnection
        self.conn = connection(p.hostname, port=p.port, timeout=timeout)
        self.last_send_time = time.time()

    def get_endpoint(self):
        return self.endpoint

    def close(self):
        self.conn.close()
        logger.debug("KLog.KHttp: closed, for next automatic connect to %s", self.endpoint)

    def do_request(self, method, url, data=None, headers=None):
        if not headers:
            headers = dict()
        headers["Connection"] = "Keep-Alive"

        logger.debug("KLog.KHttp.do_request: %s url=%s%s, body_len=%s, headers=%s",
                     method, self.endpoint, url, len(data or ""), headers)

        now = time.time()
        if now - self.last_send_time > 60:
            self.close()
        self.last_send_time = now

        try:
            self.conn.request(method, url, body=data, headers=headers)
            response = self.conn.getresponse()
            response_body = response.read()
            response_headers = response.getheaders()
            response.close()
            logger.debug("KLog.KHttp.do_request: status=%s, response_body=%s, response_headers=%s",
                         response.status, response_body, response_headers)
        except Exception as e:
            logger.debug("KLog.KHttp.do_request: Exception %s", e)
            self.close()
            return 0, None, e
        return response.status, response_body, None
