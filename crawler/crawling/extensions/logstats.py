# coding:utf-8

import logging

from twisted.internet import task
from scrapy.exceptions import NotConfigured
from scrapy import signals
import redis
import socket
import fcntl
import struct

logger = logging.getLogger(__name__)


def get_local_ip(ifname='enp1s0'):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    ret = socket.inet_ntoa(inet[20:24])
    return ret

class LogStats(object):
    """Log basic scraping stats periodically"""

    def __init__(self, stats, interval=30.0):
        self.stats = stats
        self.interval = interval
        self.multiplier = 30.0 / self.interval
        self.task = None
        self.pagesprev = 0
        self.itemsprev = 0
        self.redis_conn = redis.StrictRedis(host='192.168.1.114')

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat('LOGSTATS_INTERVAL')
        if not interval:
            raise NotConfigured
        o = cls(crawler.stats, interval)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.pagesprev = 0
        self.itemsprev = 0

        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval)

    def log(self, spider):
        items = self.stats.get_value('item_scraped_count', 0)
        pages = self.stats.get_value('response_received_count', 0)
        irate = (items - self.itemsprev) * self.multiplier
        prate = (pages - self.pagesprev) * self.multiplier
        self.pagesprev, self.itemsprev = pages, items

        msg = ("Crawled %(pages)d pages (at %(pagerate)d pages/min), "
               "scraped %(items)d items (at %(itemrate)d items/min)")
        log_args = {'pages': pages, 'pagerate': prate,
                    'items': items, 'itemrate': irate}
        logger.info(msg, log_args, extra={'spider': spider})
        self.update_weight(irate, prate, spider)

    def update_weight(self, pagesprev, itemsprev, spider):
        """
        定时更新爬虫节点权值
        :param pagesprev:
        :param itemsprev:
        :param spider:
        :return:
        """
        pending_tasks = self.redis_conn.zcard(spider.settings['job_id'])
        weight = (pagesprev + itemsprev) / pending_tasks  # 当前爬虫节点权值

        key = "weight:spider:{ip}:{job}".format(
            ip=get_local_ip(),
            job=spider.settings['job_id']
        )
        self.redis_conn.set(key, weight)

    def spider_closed(self, spider, reason):
        if self.task and self.task.running:
            self.task.stop()
