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

def put_to_queue(queue, novel_id):
    key = 'target:%s' % novel_id
    queue.put(key)
    
a = 1

def get_target(queue):
    print 'producer: ' + str(os.getpid())
    # MongoDB
    client = MongoClient()
    db = client.jingjiang
    collection = db.catalog

    while True:
        #catalogs = collection.find(limit=1).sort("create_time", 1)
        catalog = collection.find_one_and_update(
            {"status": "WAITING"}, {'$set': {"status": "PROCESSING"}}, 
            sort=[("create_time", 1)])
        #for catalog in catalogs:

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
            print info
            # 剔除 *最新章节
            if u'\xa0*\u6700\u65b0\u66f4\u65b0' in info:
                info = info[:-1]
            for i in info:
                print i
            # 摘要可能会有换行符分隔，将其合并
            if len(info) > 5:
                info[2:len(info)-2] = [''.join(info[2:len(info)-2])]
            # 摘要可能没有
            elif len(info) == 4:
                info.insert(2, '无')
            # 点击数量 暂时不需要, 接口如下（xxx 为 novelid 的值）：
            # r = requests.get('http://s8.static.jjwxc.net/getnovelclick.php?novelid=xxx')
            try:
                # chapter_link 可能被禁，导致没有
                chapter_link = tr.xpath(".//a[@itemprop='url']")[0].get('href')
            except IndexError:
                pass
                #chapter_link = chapter_link[0].get('href')
            else:
                target = {
                    #"novel_id": novel_id,
                    "chapter_id": info[0],
                    "title": info[1],
                    "abstract": info[2],
                    "word_count": int(info[3]),
                    "publish_time": info[4],
                    "chapter_link": chapter_link,
                }
                #target = {"chapter_link": chapter_link}
                # 写入 redis
                target = pickle.dumps(target)
                rpush_to_redis(target, novel_id)

        key = 'target:%s' % novel_id
        queue.put(key)
        #time.sleep(10)
        """
        global a
        a += 1
        if a == 2:
            os._exit(1)
        """
        # 写入 queue
        # put_to_queue(queue, novel_id)
        """
        try:
            rd.sadd('catalog6', i)
        except redis.exceptions.ResponseError:
            print 'error'
        """

if __name__ == '__main__':
    get_target()

