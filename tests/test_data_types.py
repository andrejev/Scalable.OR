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
from scalableor.data_types import *


class TestDateFormats(unittest.TestCase):

    def test_date_de(self):

        valid_dates = [
            "18.01.1993",
            "18.1.1993",
            "1.1.1993",
            "01.1.1993",
            "31.03.2018",
            "29.02.2016"
        ]

        invalid_dates = [
            "29.02.2017",
            "31.04.2018",
            "32.01.1990",
            "23.13.1829"
        ]

        for valid_date in valid_dates:
            self.assertTrue(date_de(valid_date))

        for invalid_date in invalid_dates:
            self.assertFalse(date_de(invalid_date))

    def test_date_iso8601(self):

        valid_dates = [
            "1993-01-18",
            "1993-1-18",
            "1993-1-1",
            "1993-1-01",
            "2018-03-31",
            "2016-02-29"
        ]

        invalid_dates = [
            "29.02.2017",
            "2017-02-29",
            "2018-04-31",
            "1990-01-32",
            "1829-13-23"
        ]

        for valid_date in valid_dates:
            self.assertTrue(date_iso8601(valid_date))

        for invalid_date in invalid_dates:
            self.assertFalse(date_iso8601(invalid_date))


class TestIPAddresses(unittest.TestCase):

    def test_ipv4(self):

        valids = ["192.168.0.1",
                  "127.0.0.1",
                  "192.168.10.101",
                  "255.255.255.255"]
        invalids = ["0.400.800"]

        for valid in valids:
            self.assertTrue(ipv4(valid))

        for invalid in invalids:
            self.assertFalse(ipv4(invalid))


class TestAircraftRegistration(unittest.TestCase):

    def test_reg_us(self):

        valids = ["N227WN",
                  "N794SW",
                  "N418WN",
                  "N783SW",
                  "N427WN",
                  "N2X"]

        invalids = ["D-AUQG",
                    "N085ED",
                    "NAOQN",
                    "N2193EF"]

        for valid in valids:
            self.assertTrue(aircraft_reg_us(valid), "{} is a valid value".format(valid))

        for invalid in invalids:
            self.assertFalse(aircraft_reg_us(invalid), "{} is an invalid value".format(invalid))