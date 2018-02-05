# coding:utf-8
import logging

logger = logging.getLogger(__name__)

def _gcd(a, b):
    if a < b:
        a, b = b, a
    while b != 0:
        temp = a % b
        a = b
        b = temp
    return a

def gcd(weights, count):
    val = weights[0]
    for i in range(1, count):
        val = _gcd(val, weights[i])
    return val

class Robin:

    def __init__(self, max_job=0, current_weight=0):
        self.last_job = -1  # 上一次调度的爬虫节点序号
        self.initial_job = 0  # 初始序号
        self.max_job = max_job  # 当前最大爬虫节点序号
        self.current_weight = current_weight  # 当前调度权值

    def choose_spider(self, spiders_weights):
        """
        根据权值调度下一个爬虫节点
        :param spiders_weights:
        :return:
        """
        if not spiders_weights:
            logger.info('当前没有爬虫运行!')
            return

        while True:
            # 找到下一个调度的爬虫节点, 如果爬虫节点宕机，直接跳过
            current_job = (self.last_job + 1) % self.max_job
            if current_job not in spiders_weights:
                self.last_job += 1
                continue
            self.last_job = current_job

            # 轮询完所有节点后，更改当前调度值
            if current_job == 0:
                self.current_weight -= gcd(spiders_weights.values(), len(spiders_weights))  # 当前调度的权值减去各个爬虫权值的最大公约数
                if self.current_weight <= 0:
                    self.current_weight = max(spiders_weights.values())
                    if self.current_weight == 0:  # 爬虫节点出现异常
                        return -1

            # 如果当前爬虫节点的权值 >= 当前调度的权值，则返回调用的节点序号
            if spiders_weights[current_job] >= self.current_weight:
                return current_job

    def update_max_job(self, max_job):
        self.max_job = max_job + 1

    def get_max_job(self):
        return self.max_job
