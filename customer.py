# -*- coding: utf-8 -*-
import os
import time
import datetime
import pickle
import re
from collections import OrderedDict
import requests
import redis
from pymongo import MongoClient
from lxml import etree

from proxies import Proxy

"""
爬取小说的整个章节

在 redis 中维护了一个名为 customer_proxies 的代理池，
使用代理去爬取二级目录。如果在爬取的过程中发现文章
需要登陆或者被锁，则直接舍弃整片文章（将其状态改为
SUSPENDED）。只有爬取到整片文章的所有章节才会写入
novel 表中，且将其状态改为 FINISHED。
"""

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.36"
    )
}

# redis
rd = redis.Redis()

def connect_to_MongoDB():
    client = MongoClient()
    db = client.jingjiang
    return db.novel, db.catalog

def process_task(collection, novel_id):
    catalog = collection.find_one_and_update(
        {"novel_id": novel_id}, {'$set': {"status": "PROCESSING"}})
    return

def suspend_task(collection, novel_id):
    catalog = collection.find_one_and_update(
        {"novel_id": novel_id}, {'$set': {"status": "SUSPENDED"}})
    return

def finish_task(collection, novel_id):
    catalog = collection.find_one_and_update(
        {"novel_id": novel_id}, {"$set":{"status": "FINISHED"}})
    return catalog["novel"]

def insert_novel(collection, novel):
    collection.insert_one(novel)
    return

def parse_target(queue, lock, log):
    # MongoDB
    novel_col, catalog_col = connect_to_MongoDB()

    while True:
        chapters = []
        task = queue.get()
        print 'customer start', os.getpid()
        novel_id = int(task.split(':')[1])

        # 结束 customer 进程
        if novel_id == -1:
            os._exit(-1)

        # 更改任务状态为 PROCESSING
        process_task(catalog_col, novel_id)        

        while True:
            ptarget = rd.lpop(task)

            if ptarget is None:
                break

            target = pickle.loads(ptarget)
            log.info(target["chapter_link"])

            while True:
                # 获取代理地址
                proxy = rd.lpop("customer_proxies")
                # 只允许一个 customer 去添加 proxy
                if rd.llen("customer_proxies") <= 10:
                    if lock.acquire(block=False):
                        Proxy("customer_proxies")
                        lock.release()
                if proxy is None:
                    time.sleep(0.5)
                    continue

                # 如果请求失败则重复发送请求，直到请求成功为止
                try:
                    r = requests.get(
                            target["chapter_link"], headers=headers,
                            timeout=5, proxies={'http': proxy})
                except Exception:
                    log.info('requests error: %s', target["chapter_link"])
                else:
                    break

            r.encoding = 'gb2312'
            html = etree.HTML(r.text)

            # 某些页面可能需要登陆，或者被锁。
            try:
                novel_text = html.xpath("//div[@class='noveltext']")[0]
            except IndexError:
                # 挂起任务
                suspend_task(catalog_col, novel_id)
                del chapters
                # 删除 redis 中的数据
                rd.delete(task)
                break
            else:
                novel_text = etree.tostring(
                    novel_text, encoding="unicode", method="html")
                # 剔除前半部分无关的内容
                novel_text = re.split(
                    r'<div style="clear:both;"></div>(\s*<div class="readsmall".*?</div>)?', 
                    novel_text)[2]
                # 剔除后半部分无关的内容
                novel_text = re.split(
                    r'<div id="favoriteshow_3".*</div>', novel_text)[0]
                # 剔除干扰部分 <font>...</font><br>
                paras = re.split(r'<font.*?<br>', novel_text)
                paras = [para.strip().replace("<br>", "\r\n") for para in paras if para]
                content = '\r\n'.join(paras)
                chapters.append(OrderedDict([
                    ("chapter_id", target["chapter_id"]),
                    ("chapter_link", target["chapter_link"]),
                    ("title", target["title"]),
                    ("abstract", target["abstract"]),
                    ("word_count", target["word_count"]),
                    ("publish_time", target["publish_time"]),
                    ("content", content),
                ]))
        
        # 更改任务状态为 FINISHED
        #if chapters:
        if 'chapters' in locals():
            novel_title = finish_task(catalog_col, novel_id)
            novel = OrderedDict([
                ("novel", novel_title),
                ("novel_id", novel_id),
                ("chapters", chapters),
                ("create_time", datetime.datetime.now()),
            ])
            insert_novel(novel_col, novel)
        print 'customer end %d, pid: %d' % (novel_id, os.getpid())

