# -*- coding: utf-8 -*-

import logging

from constant import NAME

logger = logging.getLogger(NAME.replace(".", "-"))


def set_logger(sc, verbosity=False):
    global logger
    log4j = sc._jvm.org.apache.log4j
    if not verbosity:
        log4j.LogManager.getLogger("org").setLevel(log4j.Level.WARN)
        log4j.LogManager.getLogger("akka").setLevel(log4j.Level.WARN)
    logger = log4j.LogManager.getLogger(NAME.replace(".", "-"))
