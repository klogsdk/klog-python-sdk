# coding=utf-8
from datetime import datetime
import base64
from hashlib import sha1, md5
import hmac
try:
    # python 3
    from urllib.parse import parse_qsl, urlencode
except ImportError:
    # python 2
    from urlparse import parse_qsl
    from urllib import urlencode


class KAuth:
    def __init__(self, credential):
        self.credential = credential

    @staticmethod
    def get_signature(secret_key, method, action, query_string, content_md5, content_type, date_string, klog_headers):
        lst = [method, content_md5, content_type, date_string]

        dic = {}
        for k, v in klog_headers.items():
            if k.lower().startswith("x-klog"):
                dic[k.lower()] = v

        for k in sorted(dic.keys()):
            lst.append("{}:{}".format(k, dic[k]))

        if query_string:
            tuples = parse_qsl(query_string)
            tuples.sort(key=lambda x: x[0])
            query_string = urlencode(tuples)
            lst.append("{}?{}".format(action, query_string))
        else:
            lst.append(action)

        s = "\n".join(lst).encode("ascii")
        h = hmac.new(secret_key.encode("ascii"), s, sha1)

        signature = base64.b64encode(h.digest())
        return signature.decode("ascii")

    def get_headers(self, method, action, query_string, content, content_type, klog_headers):
        m = md5()
        m.update(content)
        content_md5 = m.hexdigest()
        date_string = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

        signature = self.get_signature(self.credential.get_secret_key(), method, action, query_string,
                                       content_md5, content_type, date_string, klog_headers)
        headers = {
            "Content-MD5": content_md5,
            "Content-Type": content_type,
            "Date": date_string,
            "Authorization": "KLOG {}:{}".format(self.credential.get_access_key(), signature)
        }
        headers.update(klog_headers)
        return headers
