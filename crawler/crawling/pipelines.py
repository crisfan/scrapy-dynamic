from builtins import object
# -*- coding: utf-8 -*-

# Define your item pipelines here
from scutils.log_factory import LogFactory
import pymongo

class LoggingBeforePipeline(object):

    '''
    Logs the crawl, currently the 1st priority of the pipeline
    '''

    def __init__(self, logger):
        self.logger = logger
        self.logger.debug("Setup before pipeline")

    @classmethod
    def from_settings(cls, settings):
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

        return cls(logger)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

class KafkaPipeline(object):
    '''
    Pushes a serialized item to appropriate Kafka topics.
    '''

    def __init__(self, host, port, dbname, docname):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.docname = docname
        server = pymongo.MongoClient(port=self.port, host=self.host)
        db = server[self.dbname]
        self.db = db[self.docname]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(host=crawler.settings.get('MONGODB_HOST'),
                   port=crawler.settings.get('MONGODB_PORT'),
                   dbname=crawler.settings.get('MONGODB_DBNAME'),
                   docname=crawler.settings.get('MONGODB_DOCNAME'))

    def process_item(self, item, spider):
        """将item存储到mongodb中"""

        item = dict(item)
        item.pop('file_urls_names', '')
        self.db.insert(item)
        return item
