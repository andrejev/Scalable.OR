# -*- coding: utf-8 -*-
"""
Original description of GREL function is here:
https://github.com/OpenRefine/OpenRefine/wiki/General-Refine-Expression-Language
"""

import json
import math

import re


def not_implemented_error(*args, **kwargs):
    """
    generate error if method doesn't implemented
    """
    raise NotImplementedError("Method does not have an implementation.")


def slice_(obj, *args, **kwargs):
    return obj.slice(*args, **kwargs)


def type_(o):
    if hasattr(o.__class__, "grelname"):
        return o.__class__.grelname
    else:
        return o.__class__.__name__


def has_field(ob, item):
    if ob:
        if hasattr(ob, item):
            return True
        try:
            return item in ob
        except TypeError:
            # argument of type 'GRELCell' is not iterable
            pass
    return False


def if_(exp, true, false):
    return true if exp else false


def not_(obj):
    return GRELBoolean(not obj)


def and_(*other):
    return GRELBoolean(GRELBoolean(True) not in [not_(i) for i in other])


def or_(*other):
    return GRELBoolean(GRELBoolean(False) in [not_(i) for i in other])


class GRELBoolean(object):
    """
    This class implements GREL boolean object
    https://github.com/OpenRefine/OpenRefine/wiki/GREL-Boolean-Functions
    """
    grelname = "boolean"
    hasField = has_field

    def __new__(cls, value):
        if value is True:
            if not hasattr(cls, "true_instance"):
                cls.true_instance = super(GRELBoolean, cls).__new__(cls, value)
            return cls.true_instance
        else:
            if not hasattr(cls, "false_instance"):
                cls.false_instance = super(GRELBoolean, cls).__new__(cls, value)
            return cls.false_instance

    def __init__(self, value):
        self.value = value

    def __nonzero__(self):
        return bool(self.value)

    def __len__(self):
        return int(self.value)

    def __eq__(self, other):
        return (GRELBoolean(isinstance(other, self.__class__) and self.value is other.value) or
                GRELBoolean(self.value is True if other == "1" else False))

    def __str__(self):
        return str(self.value)


class GRELString(str):
    """
    This class implements GREL string functionality
    https://github.com/OpenRefine/OpenRefine/wiki/GREL-String-Functions
    """
    grelname = "string"
    hasField = has_field

    # Basic
    def length(self):
        return len(self)

    # Testing String Characteristics
    def startsWith(self, sub):
        return GRELBoolean(self.startswith(sub))

    def endsWith(self, sub):
        return GRELBoolean(self.endswith(sub))

    def contains(self, sub):
        return GRELBoolean(sub in self)

    # Case Conversion
    def toUppercase(self):
        return GRELString(self.upper())

    def toLowercase(self):
        return GRELString(self.lower())

    def toTitlecase(self):
        return GRELString(self.title())

    # Trimming

    def trim(self):
        return GRELString(str(self).strip())

    def strip(self):
        return GRELString(self.trim())

    def chomp(self, sub):
        return GRELString(self.rsplit(sub, 1)[0])

    # Substring

    def slice(self, from_i, to_i=None):
        return GRELString(self[from_i:] if to_i is None else self[from_i:to_i])

    def substring(self, *args, **kwatgs):
        return self.slice(*args, **kwatgs)

    def get(self, *args, **kwatgs):
        return self.slice(*args, **kwatgs)

    # Find and Replace

    def indexOf(self, sub):
        try:
            return self.index(sub)
        except ValueError:
            return -1

    def lastIndexOf(self, sub):
        try:
            return self.rindex(sub)
        except ValueError:
            return -1

    def replaceChars(self, from_ch, to_str):
        result = self
        for ch in from_ch:
            result = result.replace(ch, to_str)
        return GRELString(result)

    def match(self, regex):
        # return re.match(regex, self)
        raise NotImplementedError("match isn't implemented")

    # String Parsing and Splitting

    def toNumber(self):
        return int(self)

    def split(self, sep):
        return GRELList(str(self).split(sep))

    def splitByLengths(self, *lengths):
        from_i = to_i = 0
        result = []
        for l in lengths:
            to_i += l
            result.append(self[from_i:to_i])
            from_i += l
        return GRELList(result)

    def smartSplit(self, separator):
        raise NotImplementedError("smartSplit isn't implemented")

    def splitByCharType(self):
        raise NotImplementedError("splitByCharType isn't implemented")

    def partition(self, str_or_regex, omitFragment):
        raise NotImplementedError("partition isn't implemented")

    def rpartition(self, str_or_regex, omitFragment):
        raise NotImplementedError("rpartition isn't implemented")


        # Freebase Specific


class GRELList(list):
    """
    This class implements GREL array functionality
    https://github.com/OpenRefine/OpenRefine/wiki/GREL-Array-Functions
    """
    grelname = "list"
    hasField = has_field

    def length(self):
        return len(self)

    def slice(self, from_i, to_i=None):
        return GRELList(self[from_i:to_i] if to_i is None else self[from_i:])

    def get(self, *args, **kwargs):
        return self.slice(self, *args, **kwargs)

    def reverse(self):
        return GRELList(reversed(self)),

    def sorted(self):
        return GRELList(sorted(self))

    def sum(self):
        return sum(self)

    def join(self, sep):
        return GRELString(sep.join(self))

    def uniques(self):
        return GRELList(set(self))


class PythonCell(object):
    """
    this class implements variable cell of OR context for python executor
    https://github.com/OpenRefine/OpenRefine/wiki/Variables
    """

    def __init__(self, value, recon=None):
        self.value = value
        self.recon = recon


class PythonCells(dict):
    """
    this class implements variable cells of OR context for python executor
    https://github.com/OpenRefine/OpenRefine/wiki/Variables
    """

    def __init__(self, row, names):
        super(PythonCells, self).__init__(zip(names, [GRELCell(i) for i in row]))

    def __getattr__(self, item):
        if item in self:
            return self[item]
        else:
            return super(PythonCells, self).__getattribute__(self, item)


class PythonRow(dict):
    """
    this class implements variable row of OR context for python executor
    https://github.com/OpenRefine/OpenRefine/wiki/Variables
    """

    def __init__(self, row, names):
        super(PythonRow, self).__init__(self.context(row, names))

    def __getattr__(self, item):
        if item in self:
            return self[item]
        else:
            return super(PythonRow, self).__getattribute__(self, item)

    def context(self, row, names):
        return {
            "cells": PythonCells(row, names),
            "columnNames": names,
            "starred": False,
            "flagged": False,
            "index": 0,
            "record": None,
        }


class GRELCell(PythonCell):
    """
    this class implements variable cell of OR context for GREL executor
    https://github.com/OpenRefine/OpenRefine/wiki/Variables
    """
    grelname = "cell"
    hasField = has_field

    def __init__(self, value, recon=None):
        super(GRELCell, self).__init__(value, recon=recon)
        self.value = to_grel_object(self.value)


class GRELCells(PythonCells):
    """
    this class implements variable cells of OR context for GREL executor
    https://github.com/OpenRefine/OpenRefine/wiki/Variables
    """
    grelname = "cells"
    hasField = has_field

    def __init__(self, row, names):
        super(GRELCells, self).__init__(row, names)

    def __getattr__(self, item):
        return to_grel_object(super(GRELCells, self).__getattr__(item))


class GRELRow(PythonRow):
    """
    this class implements variable row of OR context for GREL executor
    https://github.com/OpenRefine/OpenRefine/wiki/Variables
    """
    grelname = "row"
    hasField = has_field

    def context(self, row, names):
        return {
            "cells": GRELCells(row, names),
            "columnNames": GRELList(names),
            "starred": GRELBoolean(False),
            "flagged": GRELBoolean(False),
            "index": 0,
            "record": None,
        }


# mapping of OR context function for GREL executor
GREL_GLOBAL_CONTEXT = {
    # boolean functions
    "and_": and_,
    "or_": or_,
    "not_": not_,
    # array functions
    "length": len,
    "slice": slice_,
    "get": slice_,
    "reverse": GRELList.reverse,
    "sort": GRELList.sorted,
    "sum": GRELList.sorted,
    "join": GRELList.join,
    "uniques": GRELList.uniques,
    # string functions
    # "length": GRELString.length,
    "startsWith": GRELString.startsWith,
    "endsWith": GRELString.endsWith,
    "contains": GRELString.contains,
    "toUppercase": GRELString.toUppercase,
    "toLowercase": GRELString.toLowercase,
    "toTitlecase": GRELString.toTitlecase,
    "trim": GRELString.trim,
    "strip": GRELString.strip,
    "chomp": GRELString.chomp,
    "substring": GRELString.substring,
    # "slice": GRELString.slice,
    # "get": GRELString.get,
    "indexOf": GRELString.indexOf,
    "lastIndexOf": GRELString.lastIndexOf,
    "replace": GRELString.replace,
    "replaceChars": GRELString.replaceChars,
    "match": GRELString.match,
    "toNumber": GRELString.toNumber,
    "split": GRELString.split,
    "splitByLengths": GRELString.splitByLengths,
    "smartSplit": GRELString.smartSplit,
    "partition": GRELString.partition,
    "rpartition": GRELString.rpartition,
    # Controls functions: Expression
    "if_": if_,
    # https://github.com/OpenRefine/OpenRefine/wiki/GREL-Controls
    "with_": not_implemented_error,
    "filter_": not_implemented_error,
    "forEach": not_implemented_error,
    "forEachIndex": not_implemented_error,
    "forRange": not_implemented_error,
    "forNonBlank": not_implemented_error,
    "isBlank": lambda x: not (x or len(x)),
    "isNonBlank": lambda x: not (not (x or len(x))),
    "isNull": lambda x: x is None,
    "isNumeric": lambda x: x and x.isdigit(),
    "isError": not_implemented_error,
    # math functions
    "floor": math.floor,
    "ceil": math.ceil,
    "round": round,
    "min": min,
    "max": max,
    "mod": lambda a, b: a % b,
    "ln": math.log,
    "log": math.log10,
    "exp": math.sqrt,
    "pow": math.pow,
    # other functions
    "type_": type_,
    "hasField": has_field,
    "jsonize": json.dumps,
    "parseJson": json.loads,
    # work with 2 OR projects
    "cross": not_implemented_error,
    # facet
    "facetCount": not_implemented_error,
    # html
    # https://github.com/OpenRefine/OpenRefine/wiki/GREL-Other-Functions
    "parseHtml": not_implemented_error,
    "select": not_implemented_error,
    "htmlAttr": not_implemented_error,
    "htmlText": not_implemented_error,
    "innerHtml": not_implemented_error,
    "outerHtml": not_implemented_error,
    "ownText": not_implemented_error,
    # Encoding and Hashing
    # https://github.com/OpenRefine/OpenRefine/wiki/GREL-String-Functions
    "diff": not_implemented_error,
    "escape": not_implemented_error,
    "unescape": not_implemented_error,
    "md5": not_implemented_error,
    "sha1": not_implemented_error,
    "phonetic": not_implemented_error,
    "reinterpret": not_implemented_error,
    "fingerprint": not_implemented_error,
    "ngram": not_implemented_error,
    "ngramFingerprint": not_implemented_error,
    "unicode": not_implemented_error,
    "unicodeType": not_implemented_error,
    # Freebase Specific
    # https://github.com/OpenRefine/OpenRefine/wiki/GREL-String-Functions
    "mqlKeyQuote": not_implemented_error,
    "mqlKeyUnquote": not_implemented_error,
}


def eval_python(exp, required_grel_context):
    """
    prepare GREL expression and execute it

    :param exp:         python exp
    :return:
    """
    exp = exp.replace("jython:", "", 1).strip()
    parameters = ", ".join(["%s=None" % k for k in required_grel_context.keys()])
    parameters_in = ", ".join(["%s=%s" % (k, k) for k in required_grel_context.keys()])
    # define expression function
    exec ("def exp_func(" + parameters + "):\n" +
          "\n".join(["  " + l for l in exp.split("\n")]))

    required_grel_context.update({"exp_func": exp_func})
    return eval("exp_func(" + parameters_in + ")", required_grel_context)


def to_grel_object(value):
    """
    convet python object to grel object

    :param value:       python object
    """
    if isinstance(value, basestring):
        try:
            value = GRELString(value)
        except UnicodeEncodeError:
            value = GRELString(value.encode("utf8"))
    elif isinstance(value, list):
        value = GRELList(value)
    elif isinstance(value, bool):
        value = GRELBoolean(value)
    return value


def to_python_object(value):
    """
    convert GREL object to python object

    :param value:       grel object
    """
    if isinstance(value, GRELString):
        value = str(value)
    elif isinstance(value, GRELList):
        value = list(value)
    elif isinstance(value, GRELBoolean):
        value = value.value
    return value


def eval_grel(exp, grel_context=None):
    """
    prepare GREL expression and execute it

    :param exp:         expression
    """
    # TODO: regex for expression functions (or/and/not)
    # TODO: fix control functions (if/with/forEach/...)
    # TODO: regex for control functions
    exp = exp.replace("grel:", "", 1).strip()
    exp = "return " + exp

    grel_context = grel_context or {}
    grel_context.update(GREL_GLOBAL_CONTEXT)

    for func_name in ["and", "or", "not", "type", "if"]:
        exp = exp.replace("%s(" % func_name, "%s_(" % func_name)

    find_substring_operations = re.findall("\[\d+,\d+\]", exp)
    if find_substring_operations:
        for sub in find_substring_operations:
            exp = exp.replace(sub, sub.replace(",", ":"), 1)

    return eval_python(exp, grel_context)


def eval_expression(row, position, exp, context=None, names=None):
    """
    prepare OR context and execute expression

    :param row:
    :param position:
    :param exp:
    :param context:
    :param names:
    :return:
    """
    if names is None:
        names = [str(i) for i in range(len(row))]

    context = context or {}
    if exp.startswith("jython:"):
        grow = PythonRow(row, names)
    elif exp.startswith("closure:"):
        raise NotImplementedError("closure context isn't exists")
    else:
        grow = GRELRow(row, names)
    context.update({
        "row": grow,
        "cells": grow.cells,
        "cell": grow.cells[names[position]],
        "value": grow.cells[names[position]].value,
        "recon": None,
        "record": None
    })
    if exp.startswith("jython:"):
        return eval_python(exp, context)
    else:
        return to_python_object(eval_grel(exp, context))
