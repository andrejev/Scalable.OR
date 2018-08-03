# -*- coding: utf-8 -*-

# system libs
import sys
import os
import unittest
import ConfigParser
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scalableor")))

# local imports
from scalableor.sampler import Sampler

cfg = ConfigParser.ConfigParser()
cfg.read("../config.default.ini")


class TestConfig(unittest.TestCase):

    def test_default_values(self):

        # Maximal size
        self.assertEqual(int(cfg.get("sampler", "max-size")), 1024)

        # Sample suffix
        self.assertEqual(str(cfg.get("sampler", "sample-suffix")), ".sample")


class TestCreateAndExtendSample(unittest.TestCase):

    def setUp(self):

        self.input_path = "sampler/input.csv"
        self.sample_path = "sampler/input.csv" + cfg.get("sampler", "sample-suffix")

        # Make sure the sample does not yet exist, to avoid flakiness
        self.assertFalse(os.path.isfile(self.sample_path), "Could not start test because sample from previous test runs"
                         " still exists.")

        # Set maximal size to default value
        Sampler.max_size = int(cfg.get("sampler", "max-size"))

    def tearDown(self):

        # Delete sample if it exists
        if os.path.exists(self.sample_path):
            os.remove(self.sample_path)

    def test_exceptions(self):
        """ Tests some error scenarios and checks if the correct exceptions are thrown.

        :return: None
        """

        # Test with non-existing input file
        with self.assertRaises(Exception) as context:
            Sampler("912rhgkwetg23t9tgefbasf83523tghewgl")
        self.assertEqual(str(context.exception), "Input file '912rhgkwetg23t9tgefbasf83523tghewgl' does not exist!")

    def test_create_below_maximal_size(self):
        """ When the input is below the maximal sample size, the sample should be identical to the original input.

        :return: None
        """

        # sample input
        sample = Sampler(self.input_path)
        sample.save()

        # check if sample and input are the same
        with open(self.input_path, "r") as input_file:
            with open(self.sample_path, "r") as sample_file:

                # Compare each line
                for line in input_file:
                    self.assertEqual(line, sample_file.readline())

    def test_create_above_maximal_size(self):
        """ When the input is larger than the maximal sample size, it should be cut before the size has been reached.

        :return: None
        """

        oracle_path = "sampler/above_maximal_size.csv.sample"

        # Set max size to 8KiB (in Mebibytes)
        Sampler.max_size = 0.0078125

        # sample input
        sample = Sampler(self.input_path)
        sample.save()

        # check if sample and oracle are the same
        with open(oracle_path, "r") as oracle_file:
            with open(self.sample_path, "r") as sample_file:
                # Compare each line
                for line in oracle_file:
                    self.assertEqual(line, sample_file.readline())

    def test_append_below_maximal_size(self):
        """ A new sample is created and data is appended. The result is still below the size limit.

        :return: None
        """

        rows_append = ["qui,dolore,Ut,labore,ad",
                       "cupidatat,exercitation,est,Lorem,esse",
                       "magna,ea,irure,et,reprehenderit"]

        # sample input
        sample = Sampler(self.input_path)

        # Append some records
        for row in rows_append:
            sample.append(row.split(","))

        # Append invalid row (too many values) and check whether an exception is thrown
        with self.assertRaises(Exception) as context:
            sample.append([1, 2, 3, 4, 5, 6])
        self.assertEqual(str(context.exception), "Too many columns in the record. 5 expected, 6 were given.")

        # Append invalid row (not a list) and check whether an exception is thrown
        with self.assertRaises(Exception) as context:
            sample.append("12345")
        self.assertEqual(str(context.exception), "Wrong input type. <type 'list'> expected, <type 'str'> was given.")

        sample.save()

        # check if sample and input are the same
        with open(self.sample_path, "r") as sample_file:
            actual_lines = [x for x in sample_file.readlines()]

        with open(self.input_path, "r") as input_file:
            expected_lines = [x for x in input_file.readlines()]

            # Add three additional records to the expected lines: Header (1st line), append row, remaining input rows.
            # Rows append must get newline commands
            expected_lines = [expected_lines[0]] + [x+"\n" for x in rows_append] + expected_lines[1:]

        # Check size of both lists
        self.assertEqual(len(actual_lines), len(expected_lines), "Different number of actual ({}) and expected lines "
                         "({})!".format(len(actual_lines), len(expected_lines)))

        # Compare each line
        for i in range(len(actual_lines)):
            self.assertEqual(actual_lines[i], expected_lines[i])

    def test_append_above_maximal_size(self):
        """ A new sample is created and data is appended. The result would exceed the size limit, so not every record
        from the original sample can be included.

        :return: None
        """

        oracle_path = "sampler/above_maximal_size_appended.csv.sample"

        # Set max size to ~17KiB (in Mebibytes)
        Sampler.max_size = 0.062234

        # sample input
        sample = Sampler(self.input_path)

        # Append many records
        with open("sampler/append_data.csv", "r") as rows_append:
            for row in rows_append.readlines():
                sample.append(row.replace("\n", "").split(","))
        sample.save()

        # check if sample and oracle are the same
        with open(oracle_path, "r") as oracle_file:
            with open(self.sample_path, "r") as sample_file:
                # Compare each line
                for line in oracle_file:
                    self.assertEqual(line, sample_file.readline())


class AppendToExistingSample(unittest.TestCase):

    def check_that_existing_sample_loaded(self):
        """ This is no realistic scenario, it just makes sure that when a sample exists, it is loaded.

        :return: None
        """

        input_path = "sampler/input-existing.csv"
        sample_source = "sampler/input-existing.csv.copy-sample"
        sample_path = "sampler/input-existing.csv.sample"

        # Make sure the sample does not yet exist
        self.assertFalse(os.path.exists(sample_path))

        # Copy the sample, as it might be overwritten when an error occurs
        shutil.copy(sample_source, sample_path)

        # Sample input. Now, the sampler should notice that a sample already exists and load this one instead of
        # creating a new one
        sample = Sampler(input_path)
        sample.save()

        # Check if the sample is still the same es the source it was copied from
        with open(sample_source) as source_file:
            with open(sample_path) as sample_file:

                for line in source_file.readlines():
                    self.assertEqual(line, sample_file.readline())

        # Delete the sample
        os.remove(sample_path)

