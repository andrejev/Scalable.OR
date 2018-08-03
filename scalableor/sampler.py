# -*- coding: utf-8 -*-

# system libs
import ConfigParser
import os
import numbers

# local imports


cfg = ConfigParser.ConfigParser()
cfg.read("config.ini")


class Sampler:

    # Note that the max_size is expressed in Mebibytes, so 1024*1024 Bytes!
    max_size = float(cfg.get("sampler", "max-size"))
    sample_suffix = cfg.get("sampler", "sample-suffix")

    # The delimiter character is set by sample.py, derived from the --csv-sep argument. Default is ','
    delimiter = ","

    def __init__(self, input_path):
        """ Creates a sample that can be filled with records.

        :param input_path: (str) Path of the input file
        """

        # Check if the input file exists
        if not os.path.exists(input_path):
            raise Exception("Input file '{}' does not exist!".format(input_path))

        if not isinstance(self.max_size, numbers.Number):
            raise Exception("Parameter max_size: Invalid type. Expected a number, got {}."
                            .format(type(self.max_size)))

        # The path to the new sample
        self.sample_path = input_path + self.sample_suffix

        # Check if a sample already exists
        if os.path.exists(input_path + self.sample_suffix):

            # New sample will be created from the previous sample
            self.previous_sample_path = self.sample_path

        else:

            # New sample will be created from the input file
            self.previous_sample_path = input_path

        # Get the header of the input/sample
        with open(self.previous_sample_path, "r") as previous_sample_file:
            self.header = previous_sample_file.readline().replace("\n", "").split(self.delimiter)

        # The list of rows that should be appended is still empty
        self.rows_append = []

    def append(self, row):
        """ Adds a row to the current sample.

        :param row: (list) Must have the same number of elements as all the other rows in the sample
        :return: None
        """

        # Check if input is a list
        if type(row) is not list:
            raise Exception("Wrong input type. {} expected, {} was given.".format(list, type(row)))

        # Check size
        if len(row) != len(self.header):
            raise Exception("Too many columns in the record. {} expected, {} were given."
                            .format(len(self.header), len(row)))

        # Add row
        self.rows_append.append(row)

    def save(self):
        """ Saves the sample into a file.

        :return: None
        """

        # The sample is written into a temp file and later copied
        with open(self.sample_path + ".new", "w+") as new_sample:

            # Write header first
            new_sample.write(self.delimiter.join(self.header)+"\n")

            # Write new rows first
            for row in self.rows_append:
                new_sample.write(self.delimiter.join(row)+"\n")

                # Break if the new rows have filled more than half of the maximal size for the sample
                if new_sample.tell() > (self.max_size / 2) * 1024 * 1024:
                    break

            # Write rows from previous sample
            with open(self.previous_sample_path, "r") as previous_sample:

                # Skip header
                previous_sample.readline()

                # Copy rows
                for line in previous_sample.readlines():

                    # If the current write would make the sample exceed the maximal size: stop writing
                    if new_sample.tell() + len(line) > self.max_size * 1024 * 1024:
                        break

                    new_sample.write(line)

            # Delete sample, if exists
            if os.path.exists(self.sample_path):
                os.remove(self.sample_path)

            # Move new sample to the place where the previous sample was before
            os.rename(self.sample_path + ".new", self.sample_path)

