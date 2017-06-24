#-*- coding: utf-8 -*-

import time
import datetime
import logging
import logging.config
import re
import urllib
import urlparse
import requests
import redis
from collections import OrderedDict
from lxml import etree
from pymongo import MongoClient

from proxies import Proxy

"""
爬取目录

在 redis 中维护了一个名为 catalog_proxies 的代理池，
使用代理去爬取目录，有个别目录的格式存在偏差，这里
直接舍弃。
"""

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
            "level": "DEBUG",
            "formatter": "default",
            "filename": "./log/catalog.log",
            "maxBytes": 102400,
            "backupCount": 2,
        },
    },
    "loggers": {
        "catalog": {
            "level": "DEBUG",
            "handlers": ['catalog'],
        },
    },
}

logging.config.dictConfig(LOGGING)
catalog_logger = logging.getLogger("catalog")

client = MongoClient()

rd = redis.Redis()

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
    return

    
def get_catalog_info(url):
    """
    :param url: the started url
    :return: the next url
    """
    while True:
        # 获取代理地址
        proxy = rd.lpop("catalog_proxies")
        if proxy is None:
            Proxy("catalog_proxies")
            proxy = rd.lpop("catalog_proxies")
        print proxy

        # 如果请求失败则重复发送请求，直到请求成功为止
        try:
            r = requests.get(url, headers=headers, timeout=5, 
                             proxies={'http': proxy})
            r.encoding = 'gb2312'
            html = etree.HTML(r.text)
            # 获取下一页的 link
            next_page_link = html.xpath("//div[@class='controlbar']/span[2]/a")[0].get('href')
        # 这里可能是 ConnectionError, ReadTimeout 等
        except Exception:
            catalog_logger.info("requests error: %s", url)
        else:
            break

    for tr in html.xpath("//table[@class='cytable']//tr[position()>1]"):
        try:
            # info 元素顺序依次：作者 作品 标签 风格 进度 字数 作品积分 发表时间
            #
            # 以 whitespace 进行 split，因为整个字符串中以 whitespace 打头和结尾，
            # 所以 split 后第一项元素和最后一项元素为 ''，需要剔除
            info = re.split(r'\s{4,}', tr.xpath("string(.)"), re.UNICODE)[1:][:-1]

            # 测试时发现有些栏目对不上，这里直接舍弃
            # 取标签 <a> 中 rel 属性的值作为 abstract 和 tag
            [abstract, tag]= tr.xpath(".//a[@rel]")[0].get("rel").strip().split(u"<br />标签：")

            # 测试时发现有些栏目可能都是空的，这里直接舍弃
            # xpath 中的 index 从 1 开始
            author_link = tr.xpath(".//td[1]/a")[0].get("href")
            author_link = urlparse.urljoin(r.url, author_link)
            author_link_query = urlparse.urlparse(author_link).query
            author_id = urlparse.parse_qs(author_link_query)['authorid'][0]

            novel_link = tr.xpath(".//td[2]/a")[0].get("href")
            novel_link = urlparse.urljoin(r.url, novel_link)
            novel_link_query = urlparse.urlparse(novel_link).query
            novel_id = urlparse.parse_qs(novel_link_query)['novelid'][0]
        except Exception:
            pass
        else:
            #print info[1]
            catalog_logger.info(info[1])
            # TODO:可能有些字段还是不匹配，这里直接舍弃
            try:
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
            except Exception:
                pass
            else:
                insert_catalog(catalog)


    if next_page_link is not None:
        next_page_link = urlparse.urljoin(r.url, next_page_link)
    print next_page_link
    catalog_logger.info(next_page_link)

    return next_page_link
            

if __name__ == '__main__':
    # 收费完结
    # "http://www.jjwxc.net/bookbase_slave.php?booktype=package"
    # 免费完结
    # "http://www.jjwxc.net/bookbase_slave.php?booktype=free&opt=&page=1&orderstr=4&endstr=true"
   
    url = "http://www.jjwxc.net/bookbase_slave.php?booktype=free&opt=&page=1&orderstr=4&endstr=true"
    while True:
        url = get_catalog_info(url)
        if url == None:
            catalog_logger.info("the end")
            break
