# -*- coding: utf-8 -*-

import os
import logging
import logging.config
import multiprocessing
from producer import get_target
from customer import parse_target

"""
如果是一个 producer 和三个 customer 则在 redis 中会维持 5 个任务，
在 mongodb 中会有一个 QUEUEING 和 三个 PROCESSING
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
        "producer": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "default",
            "filename": "./log/producer.log",
            "maxBytes": 102400,
            "backupCount": 2,
        },
        "customer": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "default",
            "filename": "./log/customer.log",
            "maxBytes": 102400,
            "backupCount": 2,
        },
    },
    "loggers": {
        "producer": {
            "level": "DEBUG",
            "handlers": ['producer'],
        },
        "customer": {
            "level": "DEBUG",
            "handlers": ['customer'],
        },
    },
}

logging.config.dictConfig(LOGGING)
producer_logger = logging.getLogger("producer")
customer_logger = logging.getLogger("customer")


queue = multiprocessing.Queue(maxsize=1)
lock = multiprocessing.Lock()

producer = multiprocessing.Process(
    target=get_target, args=(queue, producer_logger))
producer.start()

for i in xrange(3):
    customer = multiprocessing.Process(
        target=parse_target, args=(queue, lock, customer_logger))
    customer.start()


