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


class DataTypeManager(object):

    # This dict contains all the type check functions, accessible by their type identifier
    fn = {}

    # The type that will be returned if no other type has been identified
    default_type = "string"

    @staticmethod
    def register(identifier):
        """ Registers any function as check method for a type known under the given identifier.

        :param identifier: (str) A short and unique 'name' for the data type
        :return: None
        """

        def register_(func):
            DataTypeManager.add(identifier, func)
            return func

        return register_

    @staticmethod
    def add(identifier, func):
        DataTypeManager.fn[identifier] = func

    @staticmethod
    def infer(field):
        """ Tries to infer the type of the given field.

        :param field: (str) A field from the input data
        :return: (str) The identifier of the inferred type, or the default type if none applied
        """

        # Iterate over all registered type plugins and check if one of these applies to the field
        for identifier, func in DataTypeManager.fn.iteritems():
            if func(field):
                return identifier

        # If none of the above types applied, return the default type identifier
        return DataTypeManager.default_type

    @staticmethod
    def check_type(field, identifier):
        """ Checks if the given field is of the given type (identifier).

        :param field: (str) A field from the input data
        :param identifier: (str) The identifier of the type the given field should be of
        :return: (bool) True if the type applies, False if not
        """

        # Exceptional case: default data type
        if identifier == DataTypeManager.default_type:

            # The default data type does always apply!
            return True

        return DataTypeManager.fn[identifier](field)

    @staticmethod
    def check_row(row, types):
        """ Checks for a whole row of fields if the types apply to the fields.

        :param row: (list) The fields of the input data. Must be same size as types.
        :param types: (list) The corresponding types. Must be same size as row.
        :return: (bool) True if every field has the correct type, False if at least one field has an incorrect type.
        """

        for field, type in zip(row, types):
            if not DataTypeManager.check_type(field, type):
                return False

        return True


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
