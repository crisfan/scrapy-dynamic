# coding: utf-8

import redis
import hashmethods


class BloomFilterRedis:

    hash_list = ["rs_hash", "js_hash", "pjw_hash", "elf_hash", "bkdr_hash",
                 "sdbm_hash", "djb_hash", "dek_hash"]

    def __init__(self, key, server, hash_list=hash_list):
        self.key = key
        self.server = server
        self.hash_list = hash_list

    @classmethod
    def random_generator(cls, hash_value):
        """
        将hash函数得出的函数值映射到[0, 2^32-1]区间内
        :param hash_value:
        :return:
        """
        return hash_value % (1 << 32)

    def do_filter(self, item):
        """
        检查是否是新的条目，是新条目则更新bitmap并返回True，是重复条目则返回False
        :param item:
        :return:
        """
        flag = False
        for hash_func_str in self.hash_list:
            hash_func = getattr(hashmethods, hash_func_str)
            hash_value = hash_func(item)
            real_value = BloomFilterRedis.random_generator(hash_value)
            if self.server.getbit(self.key, real_value) == 0:
                self.server.setbit(self.key, real_value, 1)
                flag = True
        return flag


if __name__ == "__main__":
    bloomFilterRedis = BloomFilterRedis("bloom")
    if not bloomFilterRedis.do_filter("one item to check"):
        print '重复了哦! 不要放一些重复的东西来测试我'