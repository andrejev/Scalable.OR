# -*- coding: utf-8 -*-
import csv, os, sys, unittest

from StringIO import StringIO
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scalableor")))

import scalableor
from scalableor.exception import *

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

def run_scalable_or(argv):

    # Define files, they are not important, as Scalable.OR will crash befor reading them
    file_in = "exception/generic_file_in.csv"
    file_or = "exception/generic_file_or.json"
    file_out = "exception/generic_file_out.csv"

    # Define arguments
    argv = [] if argv is None else argv
    argv = argv + ["-i", file_in, "-p", file_or, "-o", file_out, "-l"]

    scalableor.run(argv)


class TestImport(unittest.TestCase):

    def test_empty_separator(self):
        """ Test whether an exception is thrown when the CSV separator is empty. """

        # Redirect stdout to own variable
        sys.stdout = stdout_new = StringIO()

        # Run SOR with not separator specified
        run_scalable_or(["--csv-sep", ""])
        result = stdout_new.getvalue()

        # Restore regular stdout
        stdout_new.close()
        sys.stdout = sys.__stdout__

        # Check error message
        oracle = "Scalable.OR stopped working due to an error in scalableor/import: No CSV separator specified.\n"
        self.assertEqual(oracle, result)

    def test_invalid_separator(self):
        """ Tests whether an exception is thrown when the CSV separator is invalid. """

        # Redirect stdout to own variable
        sys.stdout = stdout_new = StringIO()

        # Run SOR with not separator specified
        run_scalable_or(["--csv-sep", ";-."])
        result = stdout_new.getvalue()

        # Restore regular stdout
        stdout_new.close()
        sys.stdout = sys.__stdout__

        # Check error message
        oracle = "Scalable.OR stopped working due to an error in scalableor/import: Delimiter cannot be more than one character: ;-..\n"
        self.assertEqual(oracle, result)
