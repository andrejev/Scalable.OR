# -*- coding: utf-8 -*-
import csv
import os
import sys
import unittest

from tempfile import NamedTemporaryFile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scalableor

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
CASES_DIR = os.path.join(CURRENT_DIR, "integration")


# generate tests
def do_test_expected(self, case, order_is_relevant=True):
    file_in = os.path.join(CASES_DIR, case, "input.csv")
    file_or = os.path.join(CASES_DIR, case, "or.json")
    file_result = os.path.join(CASES_DIR, case, "output.csv")

    file_out = NamedTemporaryFile()

    scalableor.run(argv=[
        "-i", file_in,
        "-p", file_or,
        "-o", file_out.name + ".csv",
        "-l"
    ])
    file_out = open(file_out.name + ".csv", "r")

    expected_lines = list(csv.reader(open(file_result, "r")))
    actual_lines = list(csv.reader(file_out))

    self.assertEqual(len(expected_lines), len(actual_lines))

    if order_is_relevant is False:
        expected_lines = sorted(expected_lines)
        actual_lines = sorted(actual_lines)

    for index, exp_line in enumerate(expected_lines):
        self.assertEqual(exp_line, actual_lines[index])

    file_out.close()


class TestScOR(unittest.TestCase):
    def test_import_export(self):
        return do_test_expected(self, "base-import-export")


class TestORColumnSplit(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-split")

    def test_remove_original_false(self):
        return do_test_expected(self, "core-column-split-remove-original-false")

    def test_field_lengths(self):
        return do_test_expected(self, "core-column-split-field-lengths")

    def test_regexp(self):
        return do_test_expected(self, "core-column-split-regexp")


class TestORColumnMove(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-move")


class TestORColumnRename(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-rename")


class TestORColumnRemoval(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "core-column-removal")


class TestORColumnAddition(unittest.TestCase):
    def test_python(self):
        return do_test_expected(self, "core-column-addition-jython")


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


class TestORBigTest(unittest.TestCase):
    def test_base(self):
        return do_test_expected(self, "or-demo-wiki")
