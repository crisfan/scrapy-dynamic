# coding:utf-8

from __future__ import absolute_import
from future import standard_library

from builtins import str
from builtins import object
from scrapy_splash import SplashRequest
from scrapy.conf import settings
from scrapy.utils.python import to_unicode
from scrapy.utils.reqser import request_to_dict, request_from_dict

import socket, fcntl, struct
import redis
import random
import time
import tldextract
import yaml
import sys
import socket
import re
import json
import traceback
import ketama

from kafka import KafkaProducer
from crawling.utils.dupefilter import RFPDupeFilter
from kazoo.handlers.threading import KazooTimeoutError

from scutils.method_timer import MethodTimer
from scutils.zookeeper_watcher import ZookeeperWatcher
from scutils.redis_queue import RedisPriorityQueue
from scutils.redis_throttled_queue import RedisThrottledQueue
from scutils.log_factory import LogFactory
from retrying import retry


standard_library.install_aliases()


class DistributedScheduler(object):

    redis_conn = None  # the redis connection
    queue_dict = None  # the dict of throttled queues
    spider = None  # the spider using this scheduler
    queue_keys = None  # the list of current queues
    queue_class = None  # the class to use for the queue
    dupefilter = None  # the redis dupefilter
    update_time = 0  # the last time the queues were updated
    update_ip_time = 0  # the last time the ip was updated
    update_interval = 0  # how often to update the queues
    extract = None  # the tld extractor
    hits = 0  # default number of hits for a queue
    window = 0  # default window to calculate number of hits
    ip = '127.0.0.1'  # 爬虫节点对应的IP
    old_ip = None  # the old ip for logging
    ip_update_interval = 0  # the interval to update the ip address
    add_type = None  # add spider type to redis throttle queue key
    add_ip = None  # add spider public ip to redis throttle queue key
    item_retries = 0  # the number of extra tries to get an item
    my_uuid = None  # the generated UUID for the particular scrapy process
    # Zookeeper Dynamic Config Vars
    domain_config = {}  # The list of domains and their configs
    my_id = None  # The id used to read the throttle config
    config_flag = False  # Flag to reload queues if settings are wiped too
    assign_path = None  # The base assigned configuration path to read
    zoo_client = None  # The KazooClient to manage the config
    my_assignment = None  # Zookeeper path to read actual yml config
    black_domains = []  # the domains to ignore thanks to zookeeper config

    producer = None  # Kafka消息队列中的生产者
    closed = False  # kafka连接是否关闭

    def __init__(self, server, persist, update_int, timeout, retries, logger,
                 hits, window, mod, ip_refresh, add_type, add_ip, ip_regex,
                 backlog_blacklist, queue_timeout, chose):
        self.redis_conn = server
        self.persist = persist
        self.queue_dict = {}
        self.update_interval = update_int
        self.hits = hits
        self.window = window
        self.moderated = mod
        self.rfp_timeout = timeout
        self.ip_update_interval = ip_refresh
        self.add_type = add_type
        self.add_ip = add_ip
        self.item_retires = retries
        self.logger = logger
        self.ip_regex = re.compile(ip_regex)
        self.backlog_blacklist = backlog_blacklist
        self.queue_timeout = queue_timeout
        self.chose = chose
        self.extract = tldextract.TLDExtract()

        self.job_id = None  # 标识爬虫进程
        self.paused = False  # 标识爬虫是否暂停

    def setup_zookeeper(self):
        self.assign_path = settings.get('ZOOKEEPER_ASSIGN_PATH', "")
        self.my_id = settings.get('ZOOKEEPER_ID', 'all')
        self.logger.debug("Trying to establish Zookeeper connection")
        try:
            self.zoo_watcher = ZookeeperWatcher(
                                hosts=settings.get('ZOOKEEPER_HOSTS'),
                                filepath=self.assign_path + self.my_id,
                                config_handler=self.change_config,
                                error_handler=self.error_config,
                                pointer=False, ensure=True, valid_init=True)
        except KazooTimeoutError:
            self.logger.error("Could not connect to Zookeeper")
            sys.exit(1)

        if self.zoo_watcher.ping():
            self.logger.debug("Successfully set up Zookeeper connection")
        else:
            self.logger.error("Could not ping Zookeeper")
            sys.exit(1)

    def change_config(self, config_string):
        if config_string and len(config_string) > 0:
            loaded_config = yaml.safe_load(config_string)
            self.logger.info("Zookeeper config changed", extra=loaded_config)
            self.load_domain_config(loaded_config)
            self.update_domain_queues()
        elif config_string is None or len(config_string) == 0:
            self.error_config("Zookeeper config wiped")

        self.create_throttle_queues()

    def load_domain_config(self, loaded_config):
        '''
        Loads the domain_config and sets up queue_dict

        @param loaded_config: the yaml loaded config dict from zookeeper
        '''
        self.domain_config = {}
        # vetting process to ensure correct configs
        if loaded_config:
            if 'domains' in loaded_config:
                for domain in loaded_config['domains']:
                    item = loaded_config['domains'][domain]
                    # check valid
                    if 'window' in item and 'hits' in item:
                        self.logger.debug("Added domain {dom} to loaded config"
                                          .format(dom=domain))
                        self.domain_config[domain] = item
                        # domain_config = {'wikipedia.org': {'window': 60, 'scale': 0.5, 'hits': 30}}
            if 'blacklist' in loaded_config:
                self.black_domains = loaded_config['blacklist']
                # black_domains = ['domain3.com', 'www.baidu.com']

        self.config_flag = True

    def update_domain_queues(self):
        '''
        Check to update existing queues already in memory
        new queues are created elsewhere
        '''
        for key in self.domain_config:
            final_key = "{spider_type}:{job_id}:{domain}:queue".format(
                    spider_type=self.spider.name,
                    job_id=self.job_id,
                    domain=key)
            # we already have a throttled queue for this domain, update it to new settings
            if final_key in self.queue_dict:
                self.queue_dict[final_key][0].window = float(self.domain_config[key]['window'])
                self.logger.debug("Updated queue {q} with new config"
                                  .format(q=final_key))
                # if scale is applied, scale back; otherwise use updated hits
                if 'scale' in self.domain_config[key]:
                    # round to int
                    hits = int(self.domain_config[key]['hits'] * self.fit_scale(
                               self.domain_config[key]['scale']))
                    self.queue_dict[final_key][0].limit = float(hits)
                else:
                    self.queue_dict[final_key][0].limit = float(self.domain_config[key]['hits'])

    def error_config(self, message):
        extras = {}
        extras['message'] = message
        extras['revert_window'] = self.window
        extras['revert_hits'] = self.hits
        extras['spiderid'] = self.spider.name
        self.logger.info("Lost config from Zookeeper", extra=extras)
        # lost connection to zookeeper, reverting back to defaults
        for key in self.domain_config:
            final_key = "{name}:{domain}:queue".format(
                    name=self.spider.name,
                    domain=key)
            self.queue_dict[final_key][0].window = self.window
            self.queue_dict[final_key][0].limit = self.hits

        self.domain_config = {}

    def fit_scale(self, scale):
        '''
        @return: a scale >= 0 and <= 1
        '''
        if scale >= 1:
            return 1.0
        elif scale <= 0:
            return 0.0
        else:
            return scale

    def create_throttle_queues(self):
        """
        创建限流队列
        :return:
        """
        new_conf = self.check_config()
        queue_key = '{spider_type}:{job_id}:*:queue'.format(spider_type=self.spider.name,
                                                            job_id=self.job_id)
        self.queue_keys = self.redis_conn.keys(queue_key)
        for key in self.queue_keys:
            throttle_key = ""

            if self.add_type:
                throttle_key = self.spider.name + ":"
            if self.add_ip:
                throttle_key = throttle_key + self.ip + ":"

            the_domain = re.split(':', key)[2]
            throttle_key += the_domain

            if key not in self.queue_dict or new_conf:
                self.logger.debug("Added new Throttled Queue {q}"
                                  .format(q=key))
                q = RedisPriorityQueue(self.redis_conn, key)
                if the_domain not in self.domain_config:
                    self.queue_dict[key] = [RedisThrottledQueue(self.redis_conn, q, self.window, self.hits,
                                                                self.moderated, throttle_key, throttle_key, True),
                                            time.time()]
                else:
                    window = self.domain_config[the_domain]['window']
                    hits = self.domain_config[the_domain]['hits']
                    if 'scale' in self.domain_config[the_domain]:
                        hits = int(hits * self.fit_scale(self.domain_config[the_domain]['scale']))

                    self.queue_dict[key] = [RedisThrottledQueue(self.redis_conn, q, window, hits,
                                                                self.moderated, throttle_key, throttle_key, True),
                                            time.time()]

    def expire_queues(self):
        '''
        Expires old queue_dict keys that have not been used in a long time.
        Prevents slow memory build up when crawling lots of different domains
        '''
        curr_time = time.time()
        for key in list(self.queue_dict):
            diff = curr_time - self.queue_dict[key][1]
            if diff > self.queue_timeout:
                self.logger.debug("Expiring domain queue key " + key)
                del self.queue_dict[key]
                if key in self.queue_keys:
                    self.queue_keys.remove(key)

    def check_config(self):
        '''
        Controls configuration for the scheduler

        @return: True if there is a new configuration
        '''
        if self.config_flag:
            self.config_flag = False
            return True

        return False

    def report_self(self):
        ip = DistributedScheduler.get_local_ip()
        key = "stats:spider:{ip}:{job}".format(
            ip=ip,
            job=self.job_id
        )
        self.redis_conn.set(key, time.time())

    @staticmethod
    def get_local_ip(ifname='enp1s0'):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
        ret = socket.inet_ntoa(inet[20:24])
        return ret

    @classmethod
    def from_settings(cls, settings):
        server = redis.Redis(host=settings.get('REDIS_HOST'),
                             port=settings.get('REDIS_PORT'),
                             db=settings.get('REDIS_DB'))
        persist = settings.get('SCHEDULER_PERSIST', True)
        up_int = settings.get('SCHEDULER_QUEUE_REFRESH', 10)
        hits = settings.get('QUEUE_HITS', 10)
        window = settings.get('QUEUE_WINDOW', 60)
        mod = settings.get('QUEUE_MODERATED', False)
        timeout = settings.get('DUPEFILTER_TIMEOUT', 600)
        ip_refresh = settings.get('SCHEDULER_IP_REFRESH', 60)
        add_type = settings.get('SCHEDULER_TYPE_ENABLED', True)
        add_ip = settings.get('SCHEDULER_IP_ENABLED', False)
        retries = settings.get('SCHEUDLER_ITEM_RETRIES', 3)
        ip_regex = settings.get('IP_ADDR_REGEX', '.*')
        backlog_blacklist = settings.get('SCHEDULER_BACKLOG_BLACKLIST', True)
        queue_timeout = settings.get('SCHEDULER_QUEUE_TIMEOUT', 3600)

        my_level = settings.get('SC_LOG_LEVEL', 'INFO')
        my_name = settings.get('SC_LOGGER_NAME', 'sc-logger')
        my_output = settings.get('SC_LOG_STDOUT', True)
        my_json = settings.get('SC_LOG_JSON', False)
        my_dir = settings.get('SC_LOG_DIR', 'logs')
        my_bytes = settings.get('SC_LOG_MAX_BYTES', '10MB')
        my_file = settings.get('SC_LOG_FILE', 'main.log')
        my_backups = settings.get('SC_LOG_BACKUPS', 5)

        logger = LogFactory.get_instance(json=my_json,
                                         name=my_name,
                                         stdout=my_output,
                                         level=my_level,
                                         dir=my_dir,
                                         file=my_file,
                                         bytes=my_bytes,
                                         backups=my_backups)

        spider_ids = ['1', ]
        chose = ketama.Continuum(spider_ids)

        return cls(server, persist, up_int, timeout, retries, logger, hits,
                   window, mod, ip_refresh, add_type, add_ip, ip_regex,
                   backlog_blacklist, queue_timeout, chose)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def open(self, spider):
        self.spider = spider
        self.ip = DistributedScheduler.get_local_ip()
        # self.job_id = spider.settings['job_id']
        self.job_id = '1'
        self.spider.set_logger(self.logger)
        self.create_throttle_queues()
        self.setup_zookeeper()  # 连接zookeeper
        self.setup_kafka()  # 连接Kafka

        key = "stats:spider:{ip}:{job}".format(
            ip=DistributedScheduler.get_local_ip(),
            job=self.job_id
        )
        self.redis_conn.set(key, time.time())
        self.dupefilter = RFPDupeFilter(self.redis_conn, self.spider.name)

    def close(self, reason):
        self.logger.info("Closing Spider", {'spiderid': self.spider.name})

        # 清空爬虫队列对应的限流队列
        if not self.persist:
            self.logger.warning("Clearing crawl queues")
            for key in self.queue_keys:
                self.queue_dict[key][0].clear()

        # 清空Redis中爬虫节点状态
        ip = DistributedScheduler.get_local_ip()
        key = "stats:spider:{ip}:{job}".format(
            ip=ip,
            job=self.job_id
        )
        self.redis_conn.delete(key)
        key = "{job}:status".format(job=self.job_id)
        self.redis_conn.delete(key)

        # 关闭Kafka连接
        if self.producer is not None:
            self.logger.debug("Closing kafka producer")
            self.producer.close(timeout=10)

    def is_blacklisted(self, appid, crawlid):
        '''
        Checks the redis blacklist for crawls that should not be propagated
        either from expiring or stopped

        @return: True if the appid crawlid combo is blacklisted
        '''
        key_check = '{appid}||{crawlid}'.format(appid=appid,
                                                crawlid=crawlid)
        redis_key = self.spider.name + ":blacklist"
        return self.redis_conn.sismember(redis_key, key_check)

    def enqueue_request(self, request):
        """
        从spider模块中获取新的Request，交给Kafka消息队列
        :param request:
        :return:
        """
        if not request.dont_filter and self.dupefilter.request_seen(request):
            self.logger.debug("Request not added back to redis")
            return
        req_dict = self.request_to_dict(request)
        # 强调:  加入spider_type，job_id
        req_dict['spider_type'] = self.spider.name
        req_dict['job_id'] = self.job_id

        self._feed_to_kafka(req_dict)

    def _feed_to_kafka(self, json_item):
        @MethodTimer.timeout(settings['KAFKA_FEED_TIMEOUT'], False)
        def _feed(item):
            try:
                self.logger.debug("Sending json to kafka at " +
                                  str(settings['KAFKA_PRODUCER_TOPIC']))
                future = self.producer.send(settings['KAFKA_PRODUCER_TOPIC'],
                                            item)
                self.producer.flush()
                return True

            except Exception as e:
                self.logger.error("Lost connection to Kafka")
                return False

        return _feed(json_item)

    def request_to_dict(self, request):
        """
        将request对象转化为对应的字典返回
        :param request:
        :return:
        """
        req_dict = {
            # urls should be safe (safe_string_url)
            'url': to_unicode(request.url),
            'method': request.method,
            'headers': dict(request.headers),
            'body': request.body,
            'cookies': request.cookies,
            'meta': request.meta,
            '_encoding': request._encoding,
            'priority': request.priority,
            'dont_filter': request.dont_filter,
            'callback': None if request.callback is None else request.callback.__name__,
            'errback': None if request.errback is None else request.errback.__name__,
        }
        return req_dict

    def find_item(self):
        random.shuffle(self.queue_keys)
        count = 0

        while count <= self.item_retries:
            for key in self.queue_keys:
                if key.split(':')[2] in self.black_domains:
                    continue

                item = self.queue_dict[key][0].pop()

                if item:
                    self.queue_dict[key][1] = time.time()
                    return item
            count += 1

        return None

    def next_request(self):
        """
        从redis中取出已被序列化的任务封装成Requests，交给Engine
        :return:
        """
        if self.paused:
            return

        item = self.find_item()

        if item:
            '''考虑两种情况的Request:
                1. 被渲染后的Request
                2. 前端用户传入的Request
            '''
            if 'splash' in item['meta']:
                self.logger.debug("Crawl url: %s via %s" % (item['splash']['args']['url'], item['url']))
                req = request_from_dict(item, self.spider)
            else:
                req = SplashRequest(url=item['url'],
                                    callback=item['callback'],
                                    meta=item['meta'])
                if 'method' in item['method']:
                    req.method = item['method']
                if 'headers' in item['headers']:
                    req.headers = item['headers']
                if 'body' in item['body']:
                    req.body = item['body']
                if 'cookies' in item['cookies']:
                    req.cookies = item['cookies']
                if 'priority' in item['priority']:
                    req.priority = item['priority']
                self.logger.debug("Crawl url: %s" % item['url'])
            return req
        return None

    def status_from_redis(self):
        self.create_throttle_queues()
        self.expire_queues()

        status = self.redis_conn.get('{job}:status'.format(job=self.job_id))
        if status == 'pause':  # 暂停爬虫 && 重置一致性分布
            self.paused = True
            spiders = self.redis_conn.keys('stats:spider:*:*')
            spider_ids = []
            for spider in spiders:
                spider_ids.append(spider.split(':')[3])
            self.chose = ketama.Continuum(spider_ids)
            return
        if status == 'running':
            self.paused = False

    def parse_cookie(self, string):
        '''
        Parses a cookie string like returned in a Set-Cookie header
        @param string: The cookie string
        @return: the cookie dict
        '''
        results = re.findall('([^=]+)=([^\;]+);?\s?', string)
        my_dict = {}
        for item in results:
            my_dict[item[0]] = item[1]

        return my_dict

    def has_pending_requests(self):
        '''
        We never want to say we have pending requests
        If this returns True scrapy sometimes hangs.
        '''
        return False

    def setup_kafka(self):
        """
        创建生产者
        :return:
        """
        self.producer = self._create_producer()
        self.logger.debug("Successfully connected to Kafka")

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000)
    def _create_producer(self):
        if not self.closed:
            try:
                self.logger.debug("Creating new kafka producer using brokers: " +
                                  str(settings['KAFKA_HOSTS']))

                return KafkaProducer(bootstrap_servers=settings['KAFKA_HOSTS'],
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                     retries=3,
                                     linger_ms=settings['KAFKA_PRODUCER_BATCH_LINGER_MS'],
                                     buffer_memory=settings['KAFKA_PRODUCER_BUFFER_BYTES'])
            except KeyError as e:
                self.logger.error('Missing setting named ' + str(e),
                                  {'ex': traceback.format_exc()})
            except:
                self.logger.error("Couldn't initialize kafka producer.",
                                  {'ex': traceback.format_exc()})
            raise

if '__main__' == __name__:
    pass
