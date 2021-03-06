# -*- coding: utf-8 -*-

import cStringIO
import csv
import os

import re
import tempfile

from pyspark.sql import SQLContext

from scalableor.constant import COLUMN_NAME
from scalableor.context import eval_expression, to_grel_object
from scalableor.manager import MethodsManager

from scalableor.facet import get_facet_filter


@MethodsManager.register("scalableor/import")
def sc_or_import(cmd, sc=None, **kwargs):
    """
    import data in spark context and split rows in column

    :param cmd:         import parameters
    :param sc:          spark context object
    """
    rdd = sc.textFile(cmd["path"])
    rdd_splitted = rdd.map(lambda el: el.split(cmd["separator"]))
    sql_context = SQLContext(sc)
    df = sql_context.createDataFrame(rdd_splitted)

    for i in range(len(df.columns)):
        df = df.withColumnRenamed(df.columns[i], COLUMN_NAME % (i + 1))
    return df


@MethodsManager.register("scalableor/export")
def sc_or_export(cmd, df=None, **kwargs):
    """
    export data to file system

    :param cmd:         export parameters
    :param df:          spark data frame
    """
    tmp = tempfile.mkdtemp() + ".scalable.or"
    df.write.format("com.databricks.spark.csv").save(tmp)

    with open(cmd["path"], "w") as output:
        for fpath in sorted(os.listdir(tmp)):
            if fpath.startswith("part-"):
                with open(os.path.join(tmp, fpath), "r") as result:
                    for line in result.readlines():
                        output.write(line)


@MethodsManager.register("core/column-rename")
def core_column_rename(cmd, df, **kwargs):
    """
    rename column
    """
    return df.withColumnRenamed(cmd["oldColumnName"], cmd["newColumnName"])


@MethodsManager.register("core/column-removal")
def core_column_removal(cmd, df, **kwargs):
    """
    remove column by name
    """
    return df.drop(cmd["columnName"])


@MethodsManager.register("core/column-move")
def core_column_move(cmd, df, **kwargs):
    """
    move column to index by name
    """
    columns = df.columns[:]
    current_index = columns.index(cmd["columnName"])
    columns.insert(cmd["index"], columns.pop(current_index))

    replace_order = [i for i in range(len(columns))]
    replace_order.insert(cmd["index"], replace_order.pop(current_index))

    rdd_moved = df.rdd.map(lambda row: [row[i] for i in replace_order])
    return df.sql_ctx.createDataFrame(rdd_moved, columns)


@MethodsManager.register("core/row-removal")
def core_row_removal(cmd, df=None, **kwargs):
    """
    remove rows selected by facet filter
    """
    facet_filter = get_facet_filter(cmd, df)
    result = df.rdd.filter(lambda e: facet_filter(e) is False)
    return df.sql_ctx.createDataFrame(result, df.columns)


@MethodsManager.register("core/column-split")
def core_column_split(cmd, df=None, **kwargs):
    """
    split column by separator or field length
    """
    column_names = df.columns[:]
    pos = column_names.index(cmd["columnName"])

    before_columns = column_names[:pos]
    after_columns = column_names[pos + 1:]

    if "fieldLengths" in cmd:
        func = lambda e: \
            e[:pos + 1] + \
            tuple(to_grel_object(e[pos]).splitByLengths(*cmd["fieldLengths"])) + \
            e[pos + 1:]
    else:
        # max column logic
        if "maxColumns" in cmd:
            max_column = cmd["maxColumns"]
            if max_column == 1:
                return df
            if max_column > 1:
                max_column -= 1
        else:
            max_column = 0

        # if max column doesn't defined analyse first 20 column und select maximal column count
        if max_column < 1:
            max_column = 2
            for row in df.head(20):
                if hasattr(row[pos], "split"):
                    if cmd.get("regex") is True:
                        max_column = max(len(re.split(cmd["separator"], row[pos])), max_column)
                    else:
                        max_column = max(len(row[pos].split(cmd["separator"])), max_column)
            max_column -= 1

        # generate split callback
        add_to = ["" for _ in range(max_column + 1)]
        if cmd.get("regex") is True:
            func = lambda e: \
                e[:pos + 1] + \
                tuple((re.split(cmd["separator"], e[pos], max_column) + add_to)[:max_column + 1]) + \
                e[pos + 1:]
        else:
            func = lambda e: \
                e[:pos + 1] + \
                tuple((e[pos].split(cmd["separator"], max_column) + add_to)[:max_column + 1]) + \
                e[pos + 1:]

    result = df.sql_ctx.createDataFrame(df.rdd.map(func))

    # generate new column names
    new_column_names = (
        before_columns +
        [cmd["columnName"]] +
        ["%s %s" % (
            cmd["columnName"], i + 1) for i in range(len(result.columns) - len(df.columns))] +
        after_columns)

    for index, name in enumerate(new_column_names):
        result = result.withColumnRenamed("_%d" % (index + 1), name)

    if cmd.get("removeOriginalColumn") is True:
        result = result.drop(cmd["columnName"])
    return result


@MethodsManager.register("core/column-addition")
def core_column_addition(cmd, df, **kwargs):
    """
    create new column based on existing one
    """
    names = df.columns[:]
    position_of_column = df.columns.index(cmd["baseColumnName"])

    before_columns = df.columns[:position_of_column + 1]
    after_columns = df.columns[position_of_column + 1:]

    facet_fitler = get_facet_filter(cmd, df)

    # generate spark callback
    result_rdd = df.rdd.map(lambda e: (
        e[:position_of_column + 1] +
        ((eval_expression(e,
                          position_of_column,
                          cmd["expression"],
                          names=names),) if facet_fitler(e) else ("",)) +
        e[position_of_column + 1:]))

    return df.sql_ctx.createDataFrame(
        result_rdd,
        before_columns + [cmd["newColumnName"]] + after_columns)


@MethodsManager.register("core/text-transform")
def core_text_transform(cmd, df, **kwargs):
    """
    transform row values of selected column
    """
    names = df.columns[:]
    pos_of_column = df.columns.index(cmd["columnName"])
    facet_fitler = get_facet_filter(cmd, df)

    result_rdd = df.rdd.map(lambda e: (
        e[:pos_of_column] +
        ((eval_expression(e,
                          pos_of_column,
                          cmd["expression"],
                          names=names),) if facet_fitler(e) else (e[pos_of_column],)) +
        e[pos_of_column + 1:]))

    return df.sql_ctx.createDataFrame(result_rdd, df.columns)


@MethodsManager.register("core/mass-edit")
def core_mass_edit(cmd, df, **kwargs):
    """
    change row values of selected column using filter
    """
    pos_of_column = df.columns.index(cmd["columnName"])

    def core_mass_edit_callback(e):
        current_value = e[pos_of_column]
        new_value = None
        for edit in cmd["edits"]:
            if current_value in edit["from"]:
                new_value = edit["to"]
                break
        if new_value is None:
            new_value = current_value
        return (e[:pos_of_column] +
                (new_value,) +
                e[pos_of_column + 1:])

    return df.sql_ctx.createDataFrame(df.rdd.map(core_mass_edit_callback), df.columns)


@MethodsManager.register("core/fill-down")
def core_fill_down(cmd, df, **kwargs):
    """
    dummy function
    """
    return df
