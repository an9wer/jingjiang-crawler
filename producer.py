# -*- coding: utf-8 -*-
import os
import re
import time
import pickle
import requests
import redis
from pymongo import MongoClient
from lxml import etree

from proxies import Proxy

"""
爬取二级目录（也就是文章的章节目录）

在 redis 中维护了一个名为 producer_proxies 的代理池，
使用代理去爬取二级目录。如果文章目录中存在被锁的章节，
则直接舍弃整个文章（将其状态改为 SUSPENDED），只有完整
的文章才会写入队列中交给 customer 处理，并将其状态改为
PROCESSING。

producer 还有一个特殊的作用就是去重，将 catalog 表中重复
的文章删除，避免重复爬取。

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

def suspend_task(collection, novel_id):
    catalog = collection.find_one_and_update(
        {"novel_id": novel_id}, {'$set': {"status": "SUSPENDED"}})
    return

def delete_same_catalog(collection, novel_id):
    collection.delete_many({"novel_id": novel_id, "status": "WAITING"})
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
                queue.put("target:-1")
            queue.close()
            queue.join_thread()
            os._exit(1)

        novel_id = catalog["novel_id"]
        print 'producer start', os.getpid(), novel_id

        while True:
            # 获取代理地址
            proxy = rd.lpop("producer_proxies")
            if proxy is None:
                Proxy("producer_proxies")
                proxy = rd.lpop("producer_proxies")

            # 如果请求失败则重复发送请求，直到请求成功为止
            try:
                r = requests.get(catalog["novel_link"], headers, timeout=5,
                                 proxies={'http': proxy})
                r.encoding = 'gb2312'
                html = etree.HTML(r.text)
                trs = html.xpath("//tr[contains(@itemprop, 'chapter')]")
            except Exception:
                log.info("requests error: %s", catalog["novel_link"])
            else:
                break

        for tr in trs:
            try:
                # info 元素顺序依次为 章节 标题 摘要 字数 更新日期
                # 以 whitespace 进行 split，因为整个字符串中以 whitespace 打头和结尾，
                # 所以 split 后第一项元素和最后一项元素为 ''，需要剔除
                info = re.split(r'\s{4,}', tr.xpath("string(.)"), re.UNICODE)[1:][:-1]
                # 剔除 *最新章节
                if u'\xa0*\u6700\u65b0\u66f4\u65b0' in info:
                    info = info[:-1]
                # 摘要可能会有换行符分隔，将其合并
                if len(info) > 5:
                    info[2: len(info)-2] = [''.join(info[2: len(info)-2])]
                # 摘要可能没有
                elif len(info) == 4:
                    info.insert(2, '无')
                # 点击数量 暂时不需要, 接口如下（xxx 为 novelid 的值）：
                # r = requests.get('http://s8.static.jjwxc.net/getnovelclick.php?novelid=xxx')

                # chapter_link 可能被禁，导致没有
                chapter_link = tr.xpath(".//a[@itemprop='url']")[0].get('href')
                #print chapter_link
                log.info(chapter_link)
            except Exception:
                if 'target' in locals():
                    del target
                # 删除 redis 中的消息队列
                rd.delete('target:%s' % novel_id)
                # 更改任务状态为 SUSPENDED
                suspend_task(catalog_col, novel_id)
                break
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
        if 'target' in locals():
            key = 'target:%s' % novel_id
            queue.put(key)
            # 更改任务状态为 QUEUEING
            queue_task(catalog_col, novel_id)

        # 删除 catalog 表中相同的 document（去重）
        delete_same_catalog(catalog_col, novel_id)

if __name__ == '__main__':
    get_target()

