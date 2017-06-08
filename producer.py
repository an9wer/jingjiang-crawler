# -*- coding: utf-8 -*-
import os
import re
import time
import pickle
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

def rpush_to_redis(target, novel_id):
    key = 'target:%s' % novel_id
    rd.rpush(key, target)
    return

def put_to_queue(queue, novel_id):
    key = 'target:%s' % novel_id
    queue.put(key)
    return

def queue_task(collection, novel_id):
    collection.update_one(
        {"novel_id": novel_id}, {"$set": {"status": "QUEUEING"}})
    return 
    
def get_target(queue, log):
    # MongoDB
    client = MongoClient()
    db = client.jingjiang
    catalog_col = db.catalog

    while True:
        catalog = catalog_col.find_one(
            {"status": "WAITING"}, sort=[("create_time", 1)])

        # 结束 producer，并将结束信号传递给 customer
        if catalog == None:
            for i in xrange(3):
                queue.put('target:-1')
            queue.close()
            queue.join_thread()
            os._exit(1)

        novel_id = catalog['novel_id']
        r = requests.get(catalog['novel_link'], headers)
        r.encoding = 'gb2312'
        html = etree.HTML(r.text)
        # 最后一个章节的属性为 itemprop='chapter newestChapter'，所以需要使用 contains
        trs = html.xpath("//tr[contains(@itemprop, 'chapter')]")
        for tr in trs:
            # info 元素顺序依次为 章节 标题 摘要 字数 更新日期
            # 以 whitespace 进行 split，因为整个字符串中以 whitespace 打头和结尾，
            # 所以 split 后第一项元素和最后一项元素为 ''，需要剔除
            info = re.split(r'\s{4,}', tr.xpath("string(.)"), re.UNICODE)[1:][:-1]
            # 剔除 *最新章节
            if u'\xa0*\u6700\u65b0\u66f4\u65b0' in info:
                info = info[:-1]
            """
            for i in info:
                log.info(i)
            """
            # 摘要可能会有换行符分隔，将其合并
            if len(info) > 5:
                info[2:len(info)-2] = [''.join(info[2:len(info)-2])]
            # 摘要可能没有
            elif len(info) == 4:
                info.insert(2, '无')
            # 点击数量 暂时不需要, 接口如下（xxx 为 novelid 的值）：
            # r = requests.get('http://s8.static.jjwxc.net/getnovelclick.php?novelid=xxx')

            # chapter_link 可能被禁，导致没有
            try:
                chapter_link = tr.xpath(".//a[@itemprop='url']")[0].get('href')
                print chapter_link
                log.info(chapter_link)
            except IndexError:
                pass
                #chapter_link = chapter_link[0].get('href')
            else:
                target = {
                    "chapter_id": info[0],
                    "title": info[1],
                    "abstract": info[2],
                    "word_count": int(info[3]),
                    "publish_time": info[4],
                    "chapter_link": chapter_link,
                }
                # 写入 redis
                target = pickle.dumps(target)
                rpush_to_redis(target, novel_id)

        # 写入队列
        key = 'target:%s' % novel_id
        queue.put(key)
        # 更改任务状态为 QUEUEING
        queue_task(catalog_col, novel_id)
        """
        try:
            rd.sadd('catalog6', i)
        except redis.exceptions.ResponseError:
            print 'error'
        """

if __name__ == '__main__':
    get_target()

