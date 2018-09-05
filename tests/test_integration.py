# -*- coding: utf-8 -*-
import csv
import os
import sys
import unittest

from tempfile import NamedTemporaryFile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scalableor")))

import scalableor

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
CASES_DIR = os.path.join(CURRENT_DIR, "integration")


# generate tests
def do_test_expected(self, case, order_is_relevant=True, argv_append=None, csv_sep=","):
    file_in = os.path.join(CASES_DIR, case, "input.csv")
    file_or = os.path.join(CASES_DIR, case, "or.json")
    file_result = os.path.join(CASES_DIR, case, "output.csv")
    file_sample = file_in + ".sample"

    # The report file is only checked if an oracle ("report.txt") exists!
    file_oracle_report = os.path.join(CASES_DIR, case, "report.txt")

    # The sample file is only checked if an oracle ("sample.csv") exists!
    file_oracle_sample = os.path.join(CASES_DIR, case, "sample.csv")

    file_out = NamedTemporaryFile()
    file_report = NamedTemporaryFile()

    # These are the default arguments that cannot be changed by any test
    argv = [
        "-i", file_in,
        "-p", file_or,
        "-o", file_out.name + ".csv",
        "-l",
        "-r", file_report.name + ".txt"
    ]

    # If parameter argv_append is set, arguments can be appended
    argv = argv + argv_append if argv_append is not None else argv

    sor_obj = scalableor.run(argv)
    file_out = open(file_out.name + ".csv", "r")

    expected_lines = list(csv.reader(open(file_result, "r"), delimiter=csv_sep))
    actual_lines = list(csv.reader(file_out, delimiter=csv_sep))

    self.assertEqual(len(expected_lines), len(actual_lines))

    if order_is_relevant is False:
        expected_lines = sorted(expected_lines)
        actual_lines = sorted(actual_lines)

    for index, exp_line in enumerate(expected_lines):
        self.assertEqual(exp_line, actual_lines[index])

    # Check report (only if an oracle is provided, see above)
    if os.path.isfile(file_oracle_report):
        with open(file_report.name + ".txt", "r") as result_report:
            actual_lines = [x for x in result_report.readlines()]

        with open(file_oracle_report) as oracle_report:
            expected_lines = [x for x in oracle_report.readlines()]

        for i, expected_line in enumerate(expected_lines):
            self.assertEqual(expected_line, actual_lines[i])

    file_out.close()
    file_report.close()

    # Delete the report object, which triggers saving the report and the sample
    del sor_obj.report

    # Check sample (only if an oracle is provided, see above)
    if os.path.isfile(file_oracle_sample):
        with open(file_sample, "r") as result_sample:
            actual_lines = [x for x in result_sample.readlines()]

        with open(file_oracle_sample) as oracle_sample:
            expected_lines = [x for x in oracle_sample.readlines()]

        for i, expected_line in enumerate(expected_lines):
            self.assertEqual(expected_line, actual_lines[i])

    # Delete sample
    if os.path.exists(file_sample):
        os.remove(file_sample)


class TestScOR(unittest.TestCase):
    def test_import_export(self):
        return do_test_expected(self, "base-import-export")

    def test_import_export_separator(self):
        return do_test_expected(self, "base-import-export-separator", argv_append=["--csv-sep", ";"], csv_sep=";")


class TestORColumnSplit(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-split")

    def test_error(self):
        return do_test_expected(self, "core-column-split-error")

    def test_remove_original_false(self):
        return do_test_expected(self, "core-column-split-remove-original-false")

    def test_field_lengths(self):
        return do_test_expected(self, "core-column-split-field-lengths")

    def test_regexp(self):
        return do_test_expected(self, "core-column-split-regexp")


class TestORColumnMove(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-move")

    def test_error(self):
        return do_test_expected(self, "core-column-move-error")


class TestORColumnRename(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-rename")

    def test_with_error(self):
        return do_test_expected(self, "core-column-rename-error")


class TestORColumnRemoval(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-removal")

    def test_named_removal(self):
        return do_test_expected(self, "core-column-removal-named", argv_append=["--col-names-first-row"])

    def test_with_error(self):
        return do_test_expected(self, "core-column-removal-error")


class TestORColumnAddition(unittest.TestCase):
    def test_python(self):
        return do_test_expected(self, "core-column-addition-jython")

    def test_with_error(self):
        return do_test_expected(self, "core-column-addition-error")


class TestORTextTransform(unittest.TestCase):
    def test_python(self):
        return do_test_expected(self, "core-text-transform")

    def test_facet(self):
        return do_test_expected(self, "core-text-transform-facet")


class TestORMassEdit(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-mass-edit")


class TestORRowRemoval(unittest.TestCase):
    def test_facet_text(self):
        return do_test_expected(self, "core-row-removal")

    def test_facet_list(self):
        return do_test_expected(self, "core-row-removal-list")


"""
class TestORBigTest(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "or-demo-wiki")
"""
