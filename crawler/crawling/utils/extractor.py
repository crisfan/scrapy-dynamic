# coding:utf-8

import lxml.etree as etree
from urlparse import urljoin
from scrapy.utils.response import get_base_url


_collect_string_content = etree.XPath("string()")


class FileLinkExtractor(object):
    FILE_EXTENTION = ['.doc', '.txt', '.docx', '.rar', '.zip', '.pdf', 'xls', 'xlsx']

    @staticmethod
    def extract_links(document, response):
        base_url = get_base_url(response)
        file_urls_names = []
        for file_url_name in FileLinkExtractor._iter_links_names(document, base_url):
            file_urls_names.extend(file_url_name)
        return file_urls_names

    @staticmethod
    def _iter_links_names(document, base_url):
        scan_tag = lambda x: x == "a"
        scan_attr = lambda x: x == "href"
        for el in document.iter(etree.Element):
            if not scan_tag(el.tag):
                continue
            attribs = el.attrib
            title = _collect_string_content(el)

            for attrib in attribs:
                if not scan_attr(attrib):
                    continue
                url = attribs[attrib]
                file_url_name = [(urljoin(base_url, url), title) for ext in FileLinkExtractor.FILE_EXTENTION if url.endswith(ext)]
                if not file_url_name:
                    break
                yield file_url_name