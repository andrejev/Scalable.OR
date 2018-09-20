# -*- coding: utf-8 -*-

import os
import re
import tempfile
import ConfigParser

from pyspark.sql import SQLContext, functions

from scalableor.constant import *
from scalableor.context import eval_expression, to_grel_object
from scalableor.manager import MethodsManager

from scalableor.facet import get_facet_filter
from scalableor.exception import SORGlobalException, SOROperationException
from scalableor.data_types import *


def safe_split(var, sep, maxsplit):
    return var.split(sep, maxsplit)


def sep_missing(sep, var):
    try:
        return sep not in var
    except TypeError:
        return False


def fillna(row_index, num_cols, fill_string):
    """ Fills possible missing columns in row with fill_string.

    :param row_index: (tuple) Tuple containing (1) row that perhaps lacks columns (=elements) and (2) its index
    :param num_cols: (int) Number of columns each row is supposed to have
    :param fill_string: (str) String that is used to fill the missing fields
    :return: (list) New row with additional columns. If row already had the required number of columns, it is returned
    as it is.
    """

    row, index = row_index

    if len(row) == num_cols:
        return row, index
    elif len(row) < num_cols:
        return row + [fill_string for _ in range(num_cols - len(row))], index


@MethodsManager.register("scalableor/import")
def sc_or_import(cmd, sc=None, report=None, **kwargs):
    """
    import data in spark context and split rows in column

    :param cmd:         import parameters
    :param sc:          spark context object
    :param report:      report object (scalableor.report)
    """

    cfg = ConfigParser.ConfigParser()

    # Check if file exists
    if not os.path.exists(cmd["path"]):
        raise SORGlobalException("The input file could not be found", "scalableor/import")

    # Check separator
    if len(cmd["separator"]) == 0:
        raise SORGlobalException("No CSV separator specified", "scalableor/import")

    # If specified, read config file
    if cmd["input_cfg"] is not None:

        # Make sure the file exists!
        if not os.path.exists(cmd["input_cfg"]):
            raise SORGlobalException("The input configuration file could not be found", "scalableor/import")

        # Load the file
        with open(cmd["input_cfg"], "r") as input_cfg:
            cfg.readfp(input_cfg)

    if cmd["input_cfg"] is not None:

        # Get default values for broken_lines from input config, if exists
        broken_lines = cfg.get("general", "broken_lines").lower()
        print("Got broken lines setting from config file: {}".format(broken_lines))

    else:
        # Ask user what to do with broken lines if no config exists
        while True:
            broken_lines = raw_input("What should Scalable.OR do in case of broken lines? "
                                     "(R)emove line and append to report or (F)ill missing fields: ").lower()

            if broken_lines == "r" or broken_lines == "f":
                break
            else:
                print("Invalid option. Please choose either 'R' or 'F'!")

    # Perhaps check fill string
    if broken_lines == "f":
        fill_string = raw_input("Please enter any string to fill the missing fields. Leave empty for empty fields: ")
    else:
        fill_string = ""

    # Read CSV file as plain text file into an RDD
    rdd = sc.textFile(cmd["path"]).zipWithIndex()

    # RDD consists of a list of text lines. Now, the lines are splitted be the CSV delimiter, to create a RDD out
    # of a list of lists, whereby each list corresponds to one table row.
    rdd = rdd.map(lambda x: (x[0].split(cmd["separator"]), x[1]))

    # Get number of cols in the first row (= header row)
    num_cols = len(rdd.first()[0])

    if broken_lines == "r":
        # Remove rows that do not have the correct number of columns. Therefore, two RDDs are created. One contains all
        # valid rows (correct number of columns), and one contains invalid rows. The valid RDD (rdd) will be used for
        # further processing. The invalid RDD (invalid_rows) will be added to the report.
        invalid_rows = rdd.filter(lambda x: len(x[0]) != num_cols)
        rdd = rdd.filter(lambda x: len(x[0]) == num_cols)

        # Might be a bit inefficient... check
        # https://stackoverflow.com/questions/29547185/apache-spark-rdd-filter-into-two-rdds for improvement!

        # Add invalid rows to the report
        for row, index in invalid_rows.collect():
            report.row_error("scalableor/import", "Row has an invalid number of columns and was automatically "
                                                  "removed", row, line=index+1, sample_append=False)

    elif broken_lines == "f":
        # Fills broken lines, meaning that missing fields are substituted by a user-specified string
        rdd = rdd.map(lambda x: fillna(x, num_cols, fill_string))

    # Obtain the SQLSession from the SparkContext
    spark = SQLContext(sc).sparkSession

    # Create DataFrame
    if cmd["col_names_first_row"]:

        # If the first row contains the column names
        col_names = rdd.first()[0]

        # Remove first row (column names) from rdd
        rdd = rdd.filter(lambda x: x[0] != col_names)

    else:

        # If the first row does not conaint column names, just name them "Column 1", "Column 2" etc.
        col_names = [COLUMN_NAME % (i+1) for i in range(len(rdd.first()[0]))]

    # Infer (rich semantic) data types based on a random sample
    types_rdd = rdd.map(lambda x: [DataTypeManager.infer(y) for y in x[0]])  # use rdd.sample().map... for production!
    types = types_rdd.first()

    # For each type: ask the user if it is the correct one
    if cmd["review_types"] or cmd["input_cfg"] is not None:
        for i, type in enumerate(types):

            # Check if there is an config entry for the respective column
            if cfg.has_option("types", col_names[i]):
                new_type = cfg.get("types", col_names[i])

                # Check if a valid type has been used
                if new_type in DataTypeManager.get_registered_types():
                    types[i] = new_type
                    print("Got data type of column {} from config: {}".format(col_names[i], new_type))
                    continue

            # If it could not be successfully read from the config (--> no continue), ask the user about the type
            answer = raw_input("{} has been detected as type '{}'. Is this correct? [y/n] ".format(col_names[i], type))

            # If user chose "no": do type correction
            if answer.lower() == "n":

                available_types = ", ".join(["'" + x + "'" for x in DataTypeManager.get_registered_types()])

                while True:
                    new_type = raw_input("Sorry for that. What is the correct type? You can choose among {}. ".
                                         format(available_types))

                    # Check if a valid type was chosen
                    if new_type in DataTypeManager.get_registered_types():
                        types[i] = new_type
                        break

                    else:
                        print("Your choice '{}' is not a correct data type. You can choose among {}".
                              format(new_type, available_types))

    # Remove rows from the DataFrame that contain data of invalid types. Before, save them for the sample
    rows_wrong_data_types = rdd.filter(lambda row: not DataTypeManager.check_row(row[0], types))
    rdd = rdd.filter(lambda row: DataTypeManager.check_row(row[0], types))

    # The removed rows make up the initial sample
    for row, index in rows_wrong_data_types.collect():
        report.row_error("scalableor/import", "Wrong data type identified. Fields should be of type {}, but are {}"
                         .format(types, [DataTypeManager.infer(x) for x in row]), [x for x in row], line=index+1)

    # Remove line numbers from the RDD
    rdd = rdd.map(lambda x: x[0])

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
