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

def finish_task(collection, novel_id):
    catalog = collection.find_one_and_update(
        {"novel_id": novel_id}, {"$set":{"status": "FINISHED"}})
    return catalog["novel"]

def insert_novel(collection, novel):
    collection.insert_one(novel)
    return

def parse_target(queue, log):
    # MongoDB
    novel_col, catalog_col = connect_to_MongoDB()

    while True:
        print 'customer start'
        task = queue.get()
        novel_id = int(task.split(':')[1])

        # 结束 customer 进程
        if novel_id == -1:
            os._exit(-1)

        # 更改任务状态为 PROCESSING
        process_task(catalog_col, novel_id)        

        chapters = []
        target_num = rd.llen(task)
        for i in xrange(target_num):
            target = pickle.loads(rd.lpop(task))
            log.info(target["chapter_link"])
            r = requests.get(target["chapter_link"], headers=headers)
            r.encoding = 'gb2312'
            html = etree.HTML(r.text)

            # 某些页面可能需要登陆，或者被锁。
            try:
                novel_text = html.xpath("//div[@class='noveltext']")[0]
            except IndexError:
                pass
            else:
                novel_text = etree.tostring(novel_text, encoding="unicode", method="html")
                # 剔除前半部分无关的内容
                novel_text = re.split(r'<div style="clear:both;"></div>(\s*<div class="readsmall".*?</div>)?', novel_text)[2]
                # 剔除后半部分无关的内容
                novel_text = re.split(r'<div id="favoriteshow_3".*</div>', novel_text)[0]
                # 剔除干扰部分 <font>...</font><br>
                paras = re.split(r'<font.*?<br>', novel_text)
                #print novel_text
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
                #f.write(content.encode('utf-8'))
        
        # 更改任务状态为 FINISHED
        novel_title = finish_task(catalog_col, novel_id)
        novel = OrderedDict([
            ("novel", novel_title),
            ("novel_id", novel_id),
            ("chapters", chapters),
            ("create_time", datetime.datetime.now()),
        ])
        insert_novel(novel_col, novel)
        print 'customer end %d' % novel_id

