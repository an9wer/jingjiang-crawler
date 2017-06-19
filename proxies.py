import sys
import requests
import redis
from lxml import etree

class Proxy(object):
    def __init__(self, pool_name):
        self.pool_name = pool_name
        #self.proxies = []
        self.redis = redis.Redis()

    def get_proxies(self):
        r = requests.get("https://free-proxy-list.net/anonymous-proxy.html", 
                         #proxies={'https': 'http://67.207.89.177:3128'},
                         verify=False)
        html = etree.HTML(r.text)
        ips = html.xpath("//tbody//td[1]")
        ports = html.xpath("//tbody//td[2]")
        anonymity = html.xpath("//tbody//td[5]")
        proxies = ["http://%s:%s" % (i.text, p.text) 
                       for i, p, a in zip(ips, ports, anonymity) 
                       if a.text == "elite proxy"]

        map(lambda proxy: (self.redis.rpush(self.pool_name, proxy)),
            proxies)

        return
