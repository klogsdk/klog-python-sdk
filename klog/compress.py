# coding=utf-8

import lz4.frame


class Lz4Compressor:
    def __init__(self):
        pass

    @staticmethod
    def compress(data):
        return lz4.frame.compress(data, block_linked=False)

    @staticmethod
    def decompress(data):
        return lz4.frame.decompress(data)

    @staticmethod
    def name():
        return "lz4"
