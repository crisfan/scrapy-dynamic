# coding: utf-8

import redis
import time
import tldextract
import copy
from threading import Thread, Timer

from scutils.log_factory import LogFactory
from scutils.settings_wrapper import SettingsWrapper
from weight_round_robin import Robin

try:
    import cPickle as pickle  # PY2
except ImportError:
    import pickle


class Dispatcher:

    def __init__(self, tasks, redis_conn):

        self.tasks = tasks  # 初始URL种子队列
        self.redis_conn = redis_conn
        self.wrapper = SettingsWrapper()


        self.spiders = []  # 当前运行爬虫节点
        self.spiders_weights = None # 当前爬虫节点的权值
        self.settings = None
        self.logger = None

    def setup(self):
        """从配置文件中加载配置信息"""
        self.settings = self.wrapper.load('settings.py')
        self.logger = LogFactory.get_instance(json=self.settings['LOG_JSON'],
                                              stdout=self.settings['LOG_STDOUT'],
                                              level=self.settings['LOG_LEVEL'],
                                              name=self.settings['LOGGER_NAME'],
                                              dir=self.settings['LOG_DIR'],
                                              file=self.settings['LOG_FILE'],
                                              bytes=self.settings['LOG_MAX_BYTES'],
                                              backups=self.settings['LOG_BACKUPS'])

    def schedule_seeds(self):
        """
        分配初始种子队列
        :return:
        """
        # 加载算法
        robin = Robin()

        count = 0 # 计数器: 每100次执行一次批量操作
        pip = self.redis_conn.pipeline()

        while True:
            task_json = self.redis_conn.lpop('seeds')
            if not task_json:
                time.sleep(3)  # 等待3秒再向redis查询
                continue

            self.logger.debug('分配初始种子URLs........')
            task = pickle.loads(task_json)
            url = task['url']
            spider_type = task['spider_type']
            domain = tldextract.extract(url).domain

            # 更新加权调度算法中最大爬虫节点序号值
            spiders_weights = copy.deepcopy(self.spiders_weights)
            max_job = max(spiders_weights.keys())
            if max_job != robin.get_max_job():
                robin.update_max_job(max_job)

            job_id = robin.choose_spider(spiders_weights) # 调用加权轮叫算法选择一个爬虫节点

            if job_id == -1:  # 爬虫节点出现故障
                pass
            queue_key = '{spider_type}:{job_id}:{domain}:queue'.format(spider_type=spider_type,
                                                                       job_id=job_id,
                                                                       domain=domain)
            priority = task['priority']
            pip.zadd(queue_key, pickle.dumps(task), priority)
            count += 1
            if count == 100:
                pip.execute()
                count = 0

    def get_spiders_weights(self, interval):
        """
        定时获取各个爬虫节点的权值
        :return:
        """
        spiders_weights = dict()
        for key in self.redis_conn.keys('weight:spider:*:*'):
            job_id = int(key.split(':')[3])
            spiders_weights[job_id] = int(self.redis_conn.get(key))
        self.spiders_weights = spiders_weights
        t = Timer(interval, self.get_spiders_weights, (interval, ))
        t.start()

    def schedule_tasks(self):
        """主节点任务调度"""

        # 加载算法
        robin = Robin()

        count = 0  # 计数器: 每500次执行一次批量操作
        pip = self.redis_conn.pipeline()
        while True:
            self.logger.debug('将新加入的URLs分配给各个爬虫节点.........')
            task_json = self.redis_conn.lpop('tasks')

            task = pickle.loads(task_json)
            url = task['url']
            spider_type = task['spider_type']
            domain = tldextract.extract(url).domain

            # 更新加权调度算法中最大爬虫节点序号值
            spiders_weights = copy.deepcopy(self.spiders_weights)
            max_job = max(spiders_weights.keys())
            if max_job != robin.get_max_job():
                robin.update_max_job(max_job)

            job_id = robin.choose_spider(spiders_weights)  # 调用加权轮叫算法选择一个爬虫节点
            queue_key = '{spider_type}:{job_id}:{domain}:queue'.format(spider_type=spider_type,
                                                                       job_id=job_id,
                                                                       domain=domain)
            priority = task['priority']
            pip.zadd(queue_key, pickle.dumps(task), priority)
            count += 1
            if count == 500:
                pip.execute()
                count = 0

    def run(self):
        self.get_spiders_weights(3)

        # 分配初始种子队列
        schedule_seeds_thread = Thread(target=self.schedule_seeds)
        schedule_seeds_thread.start()
        # 分配任务队列
        # schedule_tasks_thread = Thread(target=self.schedule_tasks)
        # schedule_tasks_thread.start()


if __name__ == '__main__':
    conn = redis.Redis(host='192.168.1.114')
    d1 = Dispatcher(tasks=[], redis_conn=conn)
    d1.setup()
    d1.run()