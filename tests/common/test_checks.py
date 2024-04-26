import unittest

from liiatools.common.checks import check_year, check_term, Term


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


def test_check_term():
    assert check_term(r"October_15/2015_16/addresses.csv") == Term.OCTOBER.value
    assert check_term(r"january_16/2015_16/addresses.csv") == Term.JANUARY.value
    assert check_term(r"MAY_16/2015_16/addresses.csv") == Term.MAY.value


class TestCheckTerm(unittest.TestCase):
    def test_check_term(self):
        with self.assertRaises(ValueError):
            check_term(r"Oct_15/2015_16/addresses.csv")

    def test_check_term_2(self):
        with self.assertRaises(ValueError):
            check_term(r"/2015_16/addresses.csv")
