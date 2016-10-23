# -*- coding: utf-8 -*-

import os
import sys
import unittest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scalableor.context import eval_expression, GRELCell, \
    GRELCells, GRELRow


class TestPythonContext(unittest.TestCase):
    def test_base(self):
        tests = [
            ("jython:return value", "test"),
            ("jython:return value.replace('t', '', 1)", "est"),
            ("jython:return value.replace('t', '')", "es"),
            ("jython:return value + value", "testtest"),
            ("jython:return 't' in value", True),
        ]
        for exp, expected in tests:
            self.assertEqual(expected, eval_expression(["test"], 0, exp))

    def test_without_result(self):
        self.assertEqual(None, eval_expression(["test"], 0, "jython:1"))

    def test_import(self):
        exp = ("""import re\n"""
               """return re.findall("test", value)""")
        self.assertEqual(["test"], eval_expression(["test"], 0, "jython:" + exp))

    def test_global_context(self):
        self.assertEqual(1, eval_expression(["test"], 0, "jython:return gl_var", {"gl_var": 1}))


class TestGRELValue(unittest.TestCase):
    def test_base(self):
        self.assertEqual("test", eval_expression(["test"], 0, "value"))


class TestGRELString(unittest.TestCase):
    def test_to_titlecase(self):
        self.assertEqual("Test", eval_expression(["test"], 0, "value.toTitlecase()"))
        self.assertEqual("Test", eval_expression(["test"], 0, "toTitlecase(value)"))

    def test_to_uppercase(self):
        self.assertEqual("TEST", eval_expression(["test"], 0, "value.toUppercase()"))
        self.assertEqual("TEST", eval_expression(["tEst"], 0, "toUppercase(value)"))

    def test_startswith(self):
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "value.startsWith('Hei')"))
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "startsWith(value, 'Hei')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "startsWith(value, 'hei')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "value.startsWith('berg')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "startsWith(value, 'berg')"))

    def test_endswith(self):
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "value.endsWith('berg')"))
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "endsWith(value, 'berg')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "endsWith(value, 'berG')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "value.endsWith('Hei')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "endsWith(value, 'Hei')"))

    def test_contains(self):
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "value.contains('berg')"))
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "value.contains('Hei')"))
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "value.contains('del')"))
        self.assertEqual(True, eval_expression(["Heidelberg"], 0, "contains(value, 'del')"))
        self.assertEqual(False, eval_expression(["Heidelberg"], 0, "value.contains('hei')"))

    def test_to_lowercase(self):
        self.assertEqual("test", eval_expression(["TEST"], 0, "value.toLowercase()"))
        self.assertEqual("test", eval_expression(["tEst"], 0, "toLowercase(value)"))

    def test_trim(self):
        self.assertEqual("test", eval_expression(["test  "], 0, "value.trim()"))
        self.assertEqual("test", eval_expression(["  test"], 0, "value.trim()"))
        self.assertEqual("test", eval_expression(["  test  "], 0, "value.trim()"))

        self.assertEqual("test", eval_expression(["  test  "], 0, "trim(value)"))
        self.assertEqual("test", eval_expression(["test"], 0, "trim(value)"))

    def test_strip(self):
        self.assertEqual("test", eval_expression(["test  "], 0, "value.strip()"))
        self.assertEqual("test", eval_expression(["  test"], 0, "value.strip()"))
        self.assertEqual("test", eval_expression(["  test  "], 0, "value.strip()"))

        self.assertEqual("test", eval_expression(["  test  "], 0, "strip(value)"))
        self.assertEqual("test", eval_expression(["test"], 0, "strip(value)"))

    def test_chomp(self):
        self.assertEqual("test", eval_expression(["testly"], 0, "value.chomp('ly')"))
        self.assertEqual("test", eval_expression(["test"], 0, "value.chomp('ly')"))
        self.assertEqual("test", eval_expression(["testly"], 0, "chomp(value, 'ly')"))

    def test_length(self):
        self.assertEqual(0, eval_expression([""], 0, "value.length()"))
        self.assertEqual(10, eval_expression(["Heidelberg"], 0, "value.length()"))
        self.assertEqual(10 * 10, eval_expression(["Heidelberg" * 10], 0, "value.length()"))
        self.assertEqual(10, eval_expression(["Heidelberg"], 0, "length(value)"))

    def test_split(self):
        self.assertEqual(["Heidelberg"], eval_expression(["Heidelberg"], 0, "value.split('G')"))
        self.assertEqual(["H", "id", "lb", "rg"], eval_expression(["Heidelberg"], 0, "value.split('e')"))
        self.assertEqual(["H", "id", "lb", "rg"], eval_expression(["Heidelberg"], 0, "split(value, 'e')"))

    def test_substring(self):
        self.assertEqual("Heidelberg"[0:10], eval_expression(["Heidelberg"], 0, "value.substring(0, 10)"))
        self.assertEqual("Heidelberg"[1:10], eval_expression(["Heidelberg"], 0, "value.substring(1, 10)"))
        self.assertEqual("Heidelberg"[1:-1], eval_expression(["Heidelberg"], 0, "value.substring(1, -1)"))
        self.assertEqual("Heidelberg"[0:], eval_expression(["Heidelberg"], 0, "substring(value, 0)"))
        self.assertEqual("Heidelberg"[2:], eval_expression(["Heidelberg"], 0, "substring(value, 2)"))

    def test_slice(self):
        self.assertEqual("Heidelberg"[0:10], eval_expression(["Heidelberg"], 0, "value.slice(0, 10)"))
        self.assertEqual("Heidelberg"[1:10], eval_expression(["Heidelberg"], 0, "value.slice(1, 10)"))
        self.assertEqual("Heidelberg"[1:-1], eval_expression(["Heidelberg"], 0, "value.slice(1, -1)"))
        self.assertEqual("Heidelberg"[0:], eval_expression(["Heidelberg"], 0, "slice(value, 0)"))
        self.assertEqual("Heidelberg"[2:], eval_expression(["Heidelberg"], 0, "slice(value, 2)"))

    def test_get(self):
        self.assertEqual("Heidelberg"[0:10], eval_expression(["Heidelberg"], 0, "value.get(0, 10)"))
        self.assertEqual("Heidelberg"[1:10], eval_expression(["Heidelberg"], 0, "value.get(1, 10)"))
        self.assertEqual("Heidelberg"[1:-1], eval_expression(["Heidelberg"], 0, "value.get(1, -1)"))
        self.assertEqual("Heidelberg"[0:], eval_expression(["Heidelberg"], 0, "get(value, 0)"))
        self.assertEqual("Heidelberg"[2:], eval_expression(["Heidelberg"], 0, "get(value, 2)"))

    def test_indexOf(self):
        self.assertEqual(-1, eval_expression(["Heidelberg"], 0, "value.indexOf('G')"))
        self.assertEqual(0, eval_expression(["Heidelberg"], 0, "value.indexOf('H')"))
        self.assertEqual(6, eval_expression(["Heidelberg"], 0, "value.indexOf('berg')"))
        self.assertEqual(6, eval_expression(["Heidelberg"], 0, "indexOf(value, 'berg')"))

    def test_lastIndexOf(self):
        self.assertEqual(7, eval_expression(["Heidelberg"], 0, "value.lastIndexOf('e')"))
        self.assertEqual(7, eval_expression(["Heidelberg"], 0, "lastIndexOf(value, 'e')"))

    def test_replace(self):
        self.assertEqual("Heidelburg", eval_expression(["Heidelberg"], 0, "value.replace('berg', 'burg')"))
        self.assertEqual("Haidalbarg", eval_expression(["Heidelberg"], 0, "value.replace('e', 'a')"))
        self.assertEqual("Haidalbarg", eval_expression(["Heidelberg"], 0, "replace(value, 'e', 'a')"))

    def test_replaceChars(self):
        self.assertEqual("c ** a s ** a s", eval_expression(["c , a s ; a s"], 0, "value.replaceChars(',;', '**')"))
        self.assertEqual("c ** a s ** a s", eval_expression(["c , a s ; a s"], 0, "replaceChars(value, ',;', '**')"))
        self.assertEqual("** ** a s ** a s", eval_expression(["c , a s ; a s"], 0, "value.replaceChars(',c;', '**')"))

    def test_to_number(self):
        self.assertEqual(1, eval_expression(["1"], 0, "value.toNumber()"))
        self.assertEqual(20, eval_expression(["20"], 0, "toNumber(value)"))

    def test_split_by_lengths(self):
        self.assertEqual(["inter", "nation", "ali"],
                         eval_expression(["internationalization"], 0, "splitByLengths(value, 5, 6, 3)"))


class TestGRELBoolean(unittest.TestCase):
    def test_base(self):
        self.assertEqual(True, eval_expression([True], 0, "value"))

    def test_equal(self):
        self.assertEqual(True, eval_expression([True], 0, "value == value"))
        self.assertEqual(True, eval_expression([True], 0, "value == '1'"))

    def test_not(self):
        self.assertEqual(False, eval_expression([True], 0, "not(value)"))
        self.assertEqual(True, eval_expression([False], 0, "not(value)"))

    def test_and(self):
        self.assertEqual(True, eval_expression([True], 0, "and(value, value)"))

    def test_and_2_level(self):
        self.assertEqual(False, eval_expression([True], 0, "and(value, not(value))"))

    def test_and_expression(self):
        self.assertEqual(True, eval_expression(["1"], 0, "and(value == '1', True)"))

    def test_or(self):
        self.assertEqual(True, eval_expression([True], 0, "or(value, value)"))

    def test_or_2_level(self):
        self.assertEqual(True, eval_expression([True], 0, "or(value, not(value))"))
        self.assertEqual(False, eval_expression([True], 0, "or(not(value), not(value))"))

    def test_or_3_level(self):
        self.assertEqual(True, eval_expression([True], 0, "or(not(not(value)), not(value))"))

    def test_4_level(self):
        self.assertEqual(
            True,
            eval_expression([True], 0, "or(and(not(not(value)), value), not(value))"))


class TestGRELCell(unittest.TestCase):
    def test_isinstance(self):
        self.assertTrue(isinstance(
            eval_expression(["Heidelberg"], 0, "cell", names=["city"]), GRELCell))

    def test_value(self):
        self.assertEqual("Heidelberg",
                         eval_expression(["Heidelberg"], 0, "cell.value"))

    @unittest.skip("NotImplementedError")
    def test_recon(self):
        self.assertEqual(None,
                         eval_expression(["Heidelberg"], 0, "cell.recon"))


class TestGRELCells(unittest.TestCase):
    def test_by_name(self):
        self.assertEqual("Heidelberg",
                         eval_expression(["Heidelberg"], 0, "cells.city.value", names=["city"]))
        self.assertEqual("Heidelberg",
                         eval_expression(["Heidelberg"], 0, "cells[\"city\"].value", names=["city"]))
        self.assertEqual("test1test2",
                         eval_expression(
                             ["test1", "test2"], 0,
                             "cells.first.value + cells['second'].value",
                             names=["first", "second"]))

    def test_isinstance(self):
        self.assertTrue(isinstance(
            eval_expression(["Heidelberg"], 0, "cells", names=["city"]), GRELCells))
        self.assertTrue(isinstance(
            eval_expression(["Heidelberg"], 0, "cells[\"city\"]", names=["city"]), GRELCell))


class TestGRELRow(unittest.TestCase):
    def test_isinstance(self):
        self.assertTrue(isinstance(
            eval_expression(["Heidelberg"], 0, "row", names=["city"]), GRELRow))
        self.assertTrue(isinstance(
            eval_expression(["Heidelberg"], 0, "row.cells", names=["city"]), GRELCells))
        self.assertTrue(isinstance(
            eval_expression(["Heidelberg"], 0, "row['cells']", names=["city"]), GRELCells))

    @unittest.skip("NotImplementedError")
    def test_index(self):
        self.assertEqual(0,
                         eval_expression(["Heidelberg"], 0, "row.index"))
        self.assertEqual(0,
                         eval_expression(["Heidelberg"], 0, "row[\"index\"]"))

    @unittest.skip("NotImplementedError")
    def test_record(self):
        self.assertEqual(None,
                         eval_expression(["Heidelberg"], 0, "row.record"))

    def test_column_names(self):
        names = ["city", "street"]
        self.assertEqual(names,
                         eval_expression(["Heidelberg", "Hauptstrasse"], 0,
                                         "row.columnNames", names=names))


class TestGRELType(unittest.TestCase):
    def test_string(self):
        self.assertEqual("string",
                         eval_expression(["Heidelberg"], 0, "type(value)"))

    def test_boolean(self):
        self.assertEqual("boolean",
                         eval_expression([True], 0, "type(value)"))

    def test_row(self):
        self.assertEqual("row",
                         eval_expression(["Heidelberg"], 0, "type(row)"))

    def test_cell(self):
        self.assertEqual("cell",
                         eval_expression(["Heidelberg"], 0, "type(cell)"))

    def test_cells(self):
        self.assertEqual("cells",
                         eval_expression(["Heidelberg"], 0, "type(cells)"))


class TestGRELHasField(unittest.TestCase):
    def test_base(self):
        self.assertEqual(True,
                         eval_expression(["Heidelberg"], 0, "hasField(cell, 'value')"))

    def test_on_object(self):
        self.assertEqual(True,
                         eval_expression(["Heidelberg"], 0, "cell.hasField('value')"))
        self.assertEqual(False,
                         eval_expression(["Heidelberg"], 0, "cell.hasField('false')"))


class TestGRELIf(unittest.TestCase):
    def test_base(self):
        self.assertEqual(1,
                         eval_expression([True], 0, "if(value, 1, 0)"))
        self.assertEqual(0,
                         eval_expression([True], 0, "if(not(value), 1, 0)"))


class TestGRELSubstring(unittest.TestCase):
    def test_base(self):
        self.assertEqual("Heid",
                         eval_expression(["Heidelberg"], 0, "value[0,4]"))
