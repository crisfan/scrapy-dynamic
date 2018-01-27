# coding: utf-8

import pickle
import redis
import time
import ketama
import json
import tldextract

from scutils.log_factory import LogFactory
from scutils.settings_wrapper import SettingsWrapper

try:
    import cPickle as pickle  # PY2
except ImportError:
    import pickle


class Dispatcher:

    def __init__(self, tasks, server):

        self.tasks = tasks  # 初始URL种子队列
        self.server = server
        self.wrapper = SettingsWrapper()

        self.spiders = []  # 当前运行爬虫节点
        self.spider_count = 0  # 当前运行爬虫节点个数
        self.chose = None  # 一致性哈希分布
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

    def initial_seeds(self):
        """初始化调度器"""

        while True:
            initial_len = self.server.llen('seeds')
            if initial_len:
                break
            time.sleep(180)
            continue

        self.logger.debug('获取初始种子列表.........')
        while True:
            tasks = self.server.lrange('seeds', 0, -1)
            self.server.ltrim('seeds', -1, 0)
            self.tasks.extend(tasks)
            if self.tasks:
                break

        self.logger.debug('获取初始爬虫进程个数.........')
        self.spiders = self.server.keys('stats:spider:*:*')  # spiders列表
        self.spider_count = len(self.spiders)

        if self.spider_count:
            self.logger.debug('调用一致性哈希算法布局爬虫节点位置.......')
            job_ids = []
            for spider in self.spiders:
                job_ids.append(spider.split(':')[3])
            self.chose = ketama.Continuum(job_ids)

            self.logger.debug('分配初始种子URLs队列........')
            for task_json in self.tasks:
                task = pickle.loads(task_json)
                if 'url' in task and 'spider_type' in task:
                    extract = tldextract.TLDExtract()
                    url = task['url']
                    spider_type = task['spider_type']
                    domain = extract(url).domain
                    job_id = self.chose[url.encode('utf-8')]
                    queue_key = '{spider_type}:{job_id}:{domain}:queue'.format(spider_type=spider_type,
                                                                               job_id=job_id,
                                                                               domain=domain)
                    priority = task['priority']
                    self.server.zadd(queue_key, pickle.dumps(task), priority)
                else:
                    self.logger.error("please input url and spider_type that you want to crawl!")

    def spider_state_watcher(self):
        """监测爬虫节点是否有变化"""
        self.spiders = self.server.keys('stats:spider:*:*')
        spider_count_now = len(self.spiders)
        if spider_count_now != self.spider_count:
            self.spider_count = spider_count_now
            return True

    def center_node_dispather(self):
        """主节点任务调度"""
        while True:
            self.logger.debug('获取新加入的URLs.........')
            tasks = []
            if self.server.llen('seeds'):
                tasks.append(self.server.lpop('seeds'))
            self.tasks.extend(tasks)

            state = self.spider_state_watcher()
            if state:
                self.logger.debug('遍历爬虫节点并依次暂停当前运行的爬虫..........')
                spider_ids = []
                spider_ip_ids = []
                for spider_key in self.spiders:
                    spider_ids.append(spider_key.split(':')[3])
                    spider_ip_ids.append((spider_key.split(':')[2], spider_key.split(':')[3]))
                for spider_ip_id in spider_ip_ids:
                    key = '{job}:status'.format(job=spider_ip_id[1])
                    self.server.set(key, 'pause')

                time.sleep(4)

                self.logger.debug('由于爬虫节点状态改变，调整哈希分布...........')
                self.chose = ketama.Continuum(spider_ids)

                self.logger.debug('调整爬虫节点所负责的站点数据抓取任务, 请勿在此段时间启动额外的爬虫..........')
                queue_keys = self.server.keys('*:queue')
                for queue_key in queue_keys:
                    tasks.extend(self.server.zrange(queue_key, 0, -1))  # 获取所有爬虫队列中的urls
                    self.server.zremrangebyrank(queue_key, 0, -1)  # 清空爬虫队列

                self.logger.debug('恢复先前暂停的爬虫节点.......')
                for spider_ip_id in spider_ip_ids:
                    key = '{job}:status'.format(job=spider_ip_id[1])
                    self.server.set(key, 'running')

            self.logger.debug('等待!, 重新分配URLs..............')
            for task_json in tasks:
                task = pickle.loads(task_json)
                if 'url' in task and 'spider_type' in task:
                    extract = tldextract.TLDExtract()
                    url = task['url']
                    spider_type = task['spider_type']
                    domain = extract(url).domain
                    job_id = self.chose[url.encode('utf-8')]
                    queue_key = '{spider_type}:{job_id}:{domain}:queue'.format(spider_type=spider_type,
                                                                               job_id=job_id,
                                                                               domain=domain)
                    priority = task['priority']
                    self.server.zadd(queue_key, pickle.dumps(task), priority)
                else:
                    self.logger.error("please input url and spider_type that you want to crawl!")

    def run(self):
        """启动调度器"""
        self.initial_seeds()
        self.center_node_dispather()


if __name__ == '__main__':
    conn = redis.Redis()
    d1 = Dispatcher(tasks=[], server=conn)
    d1.setup()
    d1.run()