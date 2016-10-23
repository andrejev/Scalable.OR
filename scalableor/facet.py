# -*- coding: utf-8 -*-
import re


def get_facet_filter(cmd, df):
    """
    generate facet filter

    :param cmd:         OpenRefine command
    :param df:          Spark Dataframe object
    """
    funcs = []
    if "engineConfig" in cmd and "facets" in cmd["engineConfig"]:
        for facet_i in cmd["engineConfig"]["facets"]:
            def facet_filter(facet):
                pos = df.columns.index(facet["columnName"])
                # text facet
                if facet["type"] == "text":
                    query = facet["query"]
                    query_lower = query.lower()
                    if facet["mode"] == "text":
                        if facet["caseSensitive"] is False:
                            return lambda e: query_lower in e[pos].lower()
                        else:
                            return lambda e: query in e[pos]
                    else:
                        if facet["caseSensitive"] is False:
                            return lambda e: re.search(query, e[pos], re.IGNORECASE) is not None
                        else:
                            return lambda e: re.search(query, e[pos]) is not None
                # list facet
                elif facet["type"] == "list":
                    facet_values = []
                    for selection in facet["selection"]:
                        v = selection["v"]
                        if "v" in v:
                            facet_values.append(v["v"])
                        if "l" in v:
                            facet_values.append(v["l"])
                    return lambda e: e[pos] in facet_values
            funcs.append(facet_filter(facet_i))

    return lambda row: not len(funcs) or True in [f(row) for f in funcs]
