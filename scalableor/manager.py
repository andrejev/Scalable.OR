# -*- coding: utf-8 -*-

from functools import wraps

import log


class MethodsManager(object):
    fn = {}

    @staticmethod
    def register(name):
        def register_(func):
            MethodsManager.add(name, func)
            return func

        return register_

    @staticmethod
    def add(name, func):
        MethodsManager.fn[name] = func

    @staticmethod
    def has(name):
        return name in MethodsManager.fn

    @staticmethod
    def call(cmd, df=None, rdd=None, sc=None, report=None):
        name = cmd["op"]
        if MethodsManager.has(name):
            return MethodsManager.get(name)(cmd, df=df, rdd=rdd, sc=sc, report=report)
        else:
            raise NotImplementedError("Method '%s' doesn't found" % name)

    @staticmethod
    def get(name):
        return MethodsManager.fn[name]


class VerifiersManager(object):
    fn = {}

    @staticmethod
    def register(name):
        def register_(func):
            VerifiersManager.add(name, func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                log.logger.info("Verify '%s': begin" % name)
                func(*args, **kwargs)
                log.logger.info("Verify '%s': end" % name)
            return wrapper
        return register_

    @staticmethod
    def add(name, func):
        VerifiersManager.fn[name] = func

    @staticmethod
    def call(name, *args, **kwargs):
        return VerifiersManager.get(name)(*args, **kwargs)

    @staticmethod
    def has(name):
        return name in VerifiersManager.fn

    @staticmethod
    def get(name):
        return VerifiersManager.fn[name]
