import unittest

from liiatools.common.checks import (
    check_year,
    check_la,
)


def test_check_year():
    assert check_year("2020 SHOULD BE PLACED FOR ADOPTION Version 12") == "2020"
    assert check_year("19/20 adoption version 11") == "2020"
    assert check_year("2018/19 adoption version 11") == "2019"
    assert check_year("file_2022_ad1") == "2022"
    assert check_year("file_14032021") == "2021"
    assert check_year("file_20032021") == "2021"
    assert check_year("file_2017-18") == "2018"
    assert check_year("file_201819") == "2019"
    assert check_year("file_1920") == "2020"
    assert check_year("file_21/22") == "2022"
    assert check_year("file_version_12_18/19") == "2019"
    assert check_year("file_version_1_18/19_final") == "2019"
    assert check_year("file_version_1_1819") == "2019"


class TestCheckYear(unittest.TestCase):
    def test_check_year(self):
        with self.assertRaises(ValueError):
            check_year("file_no_year.csv")

    def test_check_year_2(self):
        with self.assertRaises(ValueError):
            check_year("1811.csv")


def test_check_la():
    assert check_la("822_2023_header.csv") == "822"
    assert check_la("935_2023_episodes.csv") == "935"


class TestCheckLA(unittest.TestCase):
    def test_check_la(self):
        with self.assertRaises(ValueError):
            check_la("file_no_la.csv")

    def test_check_la_2(self):
        with self.assertRaises(ValueError):
            check_la("SSDA903_2020_episodes.csv")
