# -*- coding: utf-8 -*-

from lxml import etree
from scrapy.selector import Selector
from scrapy_splash import SplashRequest

from crawling.utils.extractor import FileLinkExtractor
from crawling.utils.readability import Readability
from crawling.spiders.redis_spider import RedisSpider

from urlparse import urljoin


def acquire(data):
    return data[0] if data else 'the css of this field seems to be wrong'


class ZzhSpider(RedisSpider):
    name = "zzh"

    def __init__(self, *args, **kwargs):
        super(ZzhSpider, self).__init__(*args, **kwargs)

    def parse_inform_index(self, response):
        """extract the links from notification list"""

        first_page_css = response.meta['first_page_css']  # 获取首页的字段及其规则
        second_page_url = ''

        selector = Selector(response)
        # 遍历列表项
        for sel in selector.css(first_page_css.pop('list_from')):
            item = dict()
            for field, val in first_page_css.items():
                # 提取详情页链接
                if field == 'second_page_url':
                    second_page_url = item[field] = urljoin(response.url, acquire(sel.css(val).extract()))
                    continue

                # 提取首页用户自定义采集数据
                item[field] = acquire(sel.css(val).extract()) \
                    if 'attr' in val else acquire(sel.css(val).xpath('string(.)').extract())  # 提取标签属性值或文本值


            if 'second_page_css' not in response.meta:
                self.logger.info('Finish the task: %s', response.url)
                yield item

            if second_page_url:
                self.logger.info('Continue crawling the second page: %s', second_page_url)
                yield SplashRequest(url=second_page_url, meta={'item': item, 'second_page_css': response.meta['second_page_css']},
                                    callback=self.parse_inform_detail, dont_filter=False)  # 请求详情页面

        # 杜凤媛 and 万思思
        # 获取下一页，触发点击事件

    def parse_inform_detail(self, response):
        """extract details of notification"""

        second_page_css = response.meta['second_page_css']
        item = response.meta['item']  # 首页采集的数据

        # 提取详情页正文部分
        if 'body' in second_page_css:
            try:
                content = Readability(response.body, response.url).content  # 提取网页正文
                item['content'] = content  # content -> utf-8
            except Exception, e:
                self.logger.error('error: %s', e)
            return item

        # 提取详情页用户自定义数据
        for field, val in second_page_css.items():
            item[field] = acquire(response.css(val).extract())  # 提取详情页字段数据

        # get files' urls and store Files
        document = etree.HTML(response.body, parser=etree.HTMLParser(encoding='utf-8'))
        file_urls_names = FileLinkExtractor.extract_links(document, response)
        if file_urls_names:
            item['file_urls_names'] = file_urls_names
            item['exists_file'] = True
        self.logger.info('Finish the task: %s', response.url)
        return item

