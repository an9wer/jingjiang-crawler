# -*- coding: utf-8 -*-

import os
import multiprocessing
from producer import get_target
from customer import parse_target

"""
TODO:
1.  log
2.  constant
"""

print 'manage:' + str(os.getpid())
queue = multiprocessing.Queue(maxsize=3)

producer = multiprocessing.Process(target=get_target, args=(queue,))
producer.start()

for i in xrange(3):
    customer = multiprocessing.Process(target=parse_target, args=(queue,))
    customer.start()


#queue.close()
#queue.join_thread()
