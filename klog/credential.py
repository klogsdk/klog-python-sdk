# coding=utf-8


class StaticCredential:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def get_access_key(self):
        return self.access_key

    def get_secret_key(self):
        return self.secret_key
