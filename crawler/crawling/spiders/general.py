# -*- coding: utf-8 -*-

from lxml import etree
from scrapy.selector import Selector
from scrapy_splash import SplashRequest

from crawling.utils.extractor import FileLinkExtractor
from crawling.utils.readability import Readability
from crawling.spiders.redis_spider import RedisSpider

from urlparse import urljoin


class ZzhSpider(RedisSpider):
    name = "zzh"

    def __init__(self, *args, **kwargs):
        super(ZzhSpider, self).__init__(*args, **kwargs)

    def parse_inform_index(self, response):
        """extract the links from notification list"""

        if response.status != 200:
            with open("log/error.txt", 'a') as f:
                f.write('render error:' + response.url + response.status + '\n')

        field_css = response.meta

        detail_url = 'http://404.html'
        try:
            selector = Selector(response)

            for sel in selector.css(field_css['from']):  # 遍历列表项
                item = dict()
                for field, val in field_css.items():
                    if field in ['from', 'detail', 'continue', 'download_timeout', 'download_latency', 'download_slot']:
                        continue

                    if field == 'href':  # 提取详情页链接
                        detail_url = item[field] = urljoin(response.url, self.acquire(sel.css(val).extract()))
                        continue

                    item[field] = self.acquire(sel.css(val).extract()) \
                        if 'attr' in val else self.acquire(sel.css(val).xpath('string(.)').extract())  # 提取标签属性值或文本值

                con = field_css['continue']

                if con == 'false':
                    yield item

                if detail_url != 'http://404.html' and con == 'true':
                    print 'c'
                    yield SplashRequest(url=detail_url, meta={'item': item, 'detail': field_css['detail']},
                                        callback=self.parse_inform_detail, dont_filter=False)  # 请求详情页面
        except Exception, e:
            print e
            with open("log/error.txt", 'a') as f:
                f.write('parse index error' + response.url + '\n')

        # 杜凤媛 and 万思思
        # 获取下一页，触发点击事件

    def parse_inform_detail(self, response):
        """extract details of notification"""

        field_css = response.meta['detail']
        item = response.meta['item']

        if field_css['general'] == 'true':
            try:
                content = Readability(response.body, response.url).content  # 提取网页正文
                item['content'] = content  # content -> utf-8
            except Exception, e:
                with open("log/error.txt", 'a') as f:
                    f.write("parse content error:" + response.url + '\n')
            return item

        for field, val in field_css.items():
            item[field] = self.acquire(response.css(val).extract())  # 提取详情页字段数据

        # get files' urls and store Files
        document = etree.HTML(response.body, parser=etree.HTMLParser(encoding='utf-8'))
        file_urls_names = FileLinkExtractor.extract_links(document, response)
        if file_urls_names:
            item['file_urls_names'] = file_urls_names
            item['exists_file'] = True
        return item

    @staticmethod
    def acquire(data):
        return data[0] if data else 'the css of this field seems to be wrong'
