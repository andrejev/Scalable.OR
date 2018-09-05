# -*- coding: utf-8 -*-

import cStringIO
import csv
import os

import re
import tempfile

from pyspark import sql
from pyspark.sql import SQLContext, utils, functions
from collections import Counter

from scalableor.constant import *
from scalableor.context import eval_expression, to_grel_object
from scalableor.manager import MethodsManager

from scalableor.facet import get_facet_filter
from scalableor.exception import SORGlobalException, SORLocalException, SOROperationException
from scalableor.data_types import *


def safe_split(var, sep, maxsplit):
    return var.split(sep, maxsplit)


def sep_missing(sep, var):
    try:
        return sep not in var
    except TypeError:
        return False


@MethodsManager.register("scalableor/import")
def sc_or_import(cmd, sc=None, report=None, **kwargs):
    """
    import data in spark context and split rows in column

    :param cmd:         import parameters
    :param sc:          spark context object
    :param report:      report object (scalableor.report)
    """

    # Check if file exists
    if not os.path.exists(cmd["path"]):
        raise SORGlobalException("The input filed could not be found", "scalableor/import")

    # Check separator
    if len(cmd["separator"]) == 0:
        raise SORGlobalException("No CSV separator specified", "scalableor/import")

    # Read CSV file as plain text file into an RDD
    rdd = sc.textFile(cmd["path"])

    # TODO Check for encoding errors
    # TODO The report should also contain the line numbers!

    # RDD consists of a list of text lines. Now, the lines are splitted be the CSV delimiter, to create a RDD out
    # of a list of lists, whereby each list corresponds to one table row.
    rdd = rdd.map(lambda x: x.split(cmd["separator"]))

    # Get number of cols in the first row (= header row)
    num_cols = len(rdd.first())

    # Remove rows that do not have the correct number of columns. Therefore, two RDDs are created. One contains all the
    # valid rows (correct number of columns), and one contains invalid rows. The valid RDD (rdd) will be used for
    # further processing. The invalid RDD (invalid_rows) will be added to the report.
    invalid_rows = rdd.filter(lambda x: len(x) != num_cols)
    rdd = rdd.filter(lambda x: len(x) == num_cols)

    # Might be a bit inefficient... check
    # https://stackoverflow.com/questions/29547185/apache-spark-rdd-filter-into-two-rdds for improvement!

    # Add invalid rows to the report
    for row in invalid_rows.collect():
        report.row_error("scalableor/import", "Row has an invalid number of column and was automatically removed", row,
                         sample_append=False)

    # Obtain the SQLSession from the SparkContext
    spark = SQLContext(sc).sparkSession

    # Create DataFrame
    if cmd["col_names_first_row"]:

        # If the first row contains the column names
        col_names = rdd.first()

        # Remove first row (column names) from rdd
        rdd = rdd.filter(lambda x: x != col_names)

    else:

        # If the first row does not conaint column names, just name them "Column 1", "Column 2" etc.
        col_names = [COLUMN_NAME % (i+1) for i in range(len(rdd.first()))]

    # Create DF (basic column types will be inferred from the data)
    df = spark.createDataFrame(rdd, col_names)

    # Infer (rich semantic) data types based on a random sample
    types_rdd = df.rdd.map(lambda x: [DataTypeManager.infer(y) for y in x])

    # Bisher nehmen wir einfach den ersten Eintrag, weil mir nichts besseres eingefallen ist
    # TODO Each type should be the one that occurs most often
    types = types_rdd.first()

    # Remove rows from the DataFrame that contain data of invalid types
    rdd = df.rdd.filter(lambda row: DataTypeManager.check_row(row, types))

    # The removed rows make up the initial sample
    add_to_sample = df.rdd.filter(lambda row: not DataTypeManager.check_row(row, types))
    for row in add_to_sample.collect():
        report.row_error("scalableor/import", "Wrong data type identified. Fields should be of type {}, but are {}"
                         .format(types, [DataTypeManager.infer(x) for x in row]), [x for x in row])

    df = spark.createDataFrame(rdd, col_names)

    # Add empty column to store report entries
    df = df.withColumn(REPORT_COLUMN, functions.lit(""))

    return df


@MethodsManager.register("scalableor/export")
def sc_or_export(cmd, df=None, report=None, **kwargs):
    """
    export data to file system

    :param cmd:         export parameters
    :param df:          spark data frame
    :param report:      report object (scalableor.report)
    """

    # Extract row-specific report!
    # Get df that only contains lines with problems
    report_df = df.select(REPORT_COLUMN)

    # Iterate over these lines and add them to the report
    report_tmp = tempfile.mkdtemp() + ".report.scalable.or"
    report_df.write.format("com.databricks.spark.csv").save(report_tmp)

    for fpath in sorted(os.listdir(report_tmp)):
        if fpath.startswith("part-"):
            with open(os.path.join(report_tmp, fpath), "r") as report_file:
                for line in report_file:
                    if line != "":
                        operation, error, row = line.split("<->")
                        report.row_error(operation, error, row.split(REPORT_COLUMN_ROW_SEP))

    # Remove report column
    df = df.drop(REPORT_COLUMN)

    tmp = tempfile.mkdtemp() + ".scalable.or"
    df.write.format("com.databricks.spark.csv").option("delimiter", cmd["separator"]).save(tmp)

    with open(cmd["path"], "w") as output:
        for fpath in sorted(os.listdir(tmp)):
            if fpath.startswith("part-"):
                with open(os.path.join(tmp, fpath), "r") as result:

                    # Write header if specified
                    if cmd["col_names_first_row"]:
                        output.write(cmd["separator"].join(df.columns) + "\n")

                    for line in result.readlines():
                        output.write(line)


@MethodsManager.register("core/column-rename")
def core_column_rename(cmd, df, **kwargs):
    """
    rename column
    """

    # Check if the column exists
    if cmd["oldColumnName"] not in df.columns[:]:
        raise SOROperationException("Column '{}' not found".format(cmd["oldColumnName"]), "core/column-rename")

    return df.withColumnRenamed(cmd["oldColumnName"], cmd["newColumnName"])


@MethodsManager.register("core/column-removal")
def core_column_removal(cmd, df, **kwargs):
    """
    remove column by name
    """

    # Check if the column exists
    if cmd["columnName"] not in df.columns[:]:
        raise SOROperationException("Column '{}' not found".format(cmd["columnName"]), "core/column-removal")

    return df.drop(cmd["columnName"])


@MethodsManager.register("core/column-move")
def core_column_move(cmd, df, **kwargs):
    """
    move column to index by name
    """

    # Check if the column exists
    if cmd["columnName"] not in df.columns[:]:
        raise SOROperationException("Column '{}' not found".format(cmd["columnName"]), "core/column-move")

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

    # Check if the column exists
    if cmd["columnName"] not in df.columns[:]:
        raise SOROperationException("Column '{}' not found".format(cmd["columnName"]), "core/column-split")

    column_names = df.columns[:]
    pos = column_names.index(cmd["columnName"])

    # Make sure the REPORT_COLUMN is the last column. Otherwise, writing into it will not work
    assert column_names.index(REPORT_COLUMN) == len(column_names) - 1

    before_columns = column_names[:pos]
    after_columns = column_names[pos + 1:]

    # A column can either be split by length, i.e. after a specified amount of characters, or by a specified delimiter.
    if "fieldLengths" in cmd:

        # Row-wise splitting by length
        func = lambda e: \
            e[:pos + 1] + \
            tuple(to_grel_object(e[pos]).splitByLengths(*cmd["fieldLengths"])) + \
            e[pos + 1:]
    else:
        # In this block, the column is split by a specified separator (cmd["separator"])

        # The user can define the maximum amount of columns that should be created while splitting
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

        # Generate split callback. The lambda functions leave all columns before and after the specified column as they
        # are, split the specified column and, if necessary, write into the log column.
        add_to = ["" for _ in range(max_column + 1)]
        if cmd.get("regex") is True:
            func = lambda e: \
                e[:pos + 1] + \
                tuple((re.split(cmd["separator"], e[pos], max_column) + add_to)[:max_column + 1]) + \
                e[pos + 1:-1] + ("",)
        else:
            func = lambda e: \
                e[:pos + 1] + \
                tuple((safe_split(e[pos], cmd["separator"], max_column) + add_to)[:max_column + 1]) + \
                e[pos + 1:-1] + ("{}<->Notification: Cell does not contain delimiter '{}'!<->{}"
                                 .format("core/column-split", cmd["separator"], REPORT_COLUMN_ROW_SEP.join(e))
                                 if sep_missing(cmd["separator"], e[pos]) else "",)

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

    # Check if the column exists
    if cmd["baseColumnName"] not in df.columns[:]:
        raise SOROperationException("Column '{}' not found".format(cmd["baseColumnName"]), "core/column-addition")

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
