from __future__ import absolute_import
from .base_handler import BaseHandler
import tldextract
import redis
import json
import sys
from redis.exceptions import ConnectionError

try:
    import cPickle as pickle  # PY2
except ImportError:
    import pickle


class ScraperHandler(BaseHandler):

    schema = "scraper_schema.json"

    def setup(self, settings):
        '''
        Setup redis and tldextract
        '''
        self.extract = tldextract.TLDExtract()
        self.redis_conn = redis.Redis(host=settings['REDIS_HOST'],
                                      port=settings['REDIS_PORT'],
                                      db=settings.get('REDIS_DB'))

        try:
            self.redis_conn.info()
            self.logger.debug("Connected to Redis in ScraperHandler")
        except ConnectionError:
            self.logger.error("Failed to connect to Redis in ScraperHandler")
            sys.exit(1)

    def handle(self, d):
        '''
        Processes a vaild crawl request

        @param d: a valid dictionary object
        '''

        real_url = d['meta']['splash']['args']['url'] if 'splash' in d['meta'] else d['url']
        ex_res = self.extract(real_url)

        if 'job_id' in d:
            key = '{spider_type}:{job_id}:{domain}:queue'.format(spider_type=d['spider_type'],
                                                                 job_id=d['job_id'],
                                                                 domain=ex_res.domain)

            self.redis_conn.zadd(key, pickle.dumps(d), d['priority'])

        else:
            self.redis_conn.lpush('seeds', pickle.dumps(d))

        # log success
        d['parsed'] = True
        d['valid'] = True
        self.logger.info('Added crawl to Redis', extra=d)
