#-*- coding: utf-8 -*-

import logging
import logging.config
import re
import datetime
import urllib
import urlparse
import requests
from collections import OrderedDict
from lxml import etree
from pymongo import MongoClient

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": (
                "[%(asctime)s][%(filename)s:%(lineno)d]"
                "[%(levelname)s][%(name)s]: %(message)s"
            ),
        },
    },
    "handlers": {
        "catalog": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "default",
            "filename": "./log/catalog.log",
            "maxBytes": 102400,
            "backupCount": 2,
        },
    },
    "loggers": {
        "catalog": {
            "level": "INFO",
            "handlers": ['catalog'],
        },
    },
}

logging.config.dictConfig(LOGGING)
catalog_logger = logging.getLogger("catalog")

client = MongoClient()

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.36"
    )
}

def insert_catalog(catalog):
    db = client.jingjiang
    collection = db.catalog
    collection.insert_one(catalog)
    
a = 1
def get_catalog_info(url):
    r = requests.get(url, headers=headers)
    r.encoding = 'gb2312'
    # parser = etree.HTMLParser(encoding='utf-8')
    html = etree.HTML(r.text)
    # 
    for tr in html.xpath("//table[@class='cytable']//tr[position()>1]"):
        # info 元素顺序依次：作者 作品 标签 风格 进度 字数 作品积分 发表时间
        #
        # 以 whitespace 进行 split，因为整个字符串中以 whitespace 打头和结尾，
        # 所以 split 后第一项元素和最后一项元素为 ''，需要剔除
        info = re.split(r'\s{4,}', tr.xpath("string(.)"), re.UNICODE)[1:][:-1]

        # 取标签 <a> 中 rel 属性的值作为 abstract 和 tag
        [abstract, tag]= tr.xpath(".//a[@rel]")[0].get("rel").strip().split(u"<br />标签：")

        # xpath 中的 index 从 1 开始
        author_link = tr.xpath(".//td[1]/a")[0].get("href")
        author_link = urlparse.urljoin(r.url, author_link)
        author_link_query = urlparse.urlparse(author_link).query
        author_id = urlparse.parse_qs(author_link_query)['authorid'][0]

        novel_link = tr.xpath(".//td[2]/a")[0].get("href")
        novel_link = urlparse.urljoin(r.url, novel_link)
        novel_link_query = urlparse.urlparse(novel_link).query
        novel_id = urlparse.parse_qs(novel_link_query)['novelid'][0]

        print info[1]
        catalog_logger.info(info[1])
        catalog = OrderedDict([
            ("novel", info[1]),
            ("novel_id", int(novel_id)),
            ("novel_link", novel_link),
            ("author", info[0]),
            ("author_id", int(author_id)),
            ("author_link", author_link),
            ("tag", tag.strip() or u'无'), # 再次 strip() 避免 whitespace
            ("abstract", abstract or u'无'),
            ("style", info[3]),
            ("process", info[4]),
            ("word_count", int(info[5])),
            ("point", int(info[6])),
            ("publish_time", info[7]),
            ("status", 'WAITING'),   # 爬取状态
            ("create_time", datetime.datetime.now()),
        ])
        insert_catalog(catalog)

    # 获取下一页的 link
    next_page_link = html.xpath("//div[@class='controlbar']/span[2]/a")[0].get('href')
    """
    global a
    if next_page_link:
        print a
        a += 1
        if a == 2:
            import os
            os._exit(1)
    """
    next_page_link = urlparse.urljoin(r.url, next_page_link)
    print next_page_link
    catalog_logger.info(next_page_link)
    get_catalog_info(next_page_link)
            




if __name__ == '__main__':
    # 收费完结
    #start_url = "http://www.jjwxc.net/bookbase_slave.php?booktype=package"
    # 免费完结
    start_url = "http://www.jjwxc.net/bookbase_slave.php?booktype=free&opt=&page=1&orderstr=4&endstr=true"
    get_catalog_info(start_url)
