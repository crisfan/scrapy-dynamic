from scrapy.crawler import CrawlerProcess
from scrapy.conf import settings
from crawling.spiders.general import ZzhSpider


process = CrawlerProcess(settings)
process.crawl(ZzhSpider())
process.start()

