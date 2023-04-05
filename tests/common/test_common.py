from liiatools.datasets.shared_functions.common import (
    check_postcode,
    flip_dict,
    check_year,
)
from liiatools.datasets.shared_functions.converters import (
    to_short_postcode,
    to_month_only_dob,
    to_date,
)
import datetime
import unittest


def test_flip_dict():
    assert flip_dict({"key": "value"}) == {"value": "key"}


def test_check_postcode():
    assert check_postcode("AA9 4AA") == "AA9 4AA"
    assert check_postcode("   AA9 4AA   ") == "AA9 4AA"
    assert check_postcode("") == ""
    assert check_postcode("AA9         4AA") == "AA9         4AA"
    assert check_postcode("AA94AA") == "AA94AA"


def test_to_short_postcode():
    assert to_short_postcode("AA9 4AA") == "AA9 4"
    assert to_short_postcode("   AA9 4AA   ") == "AA9 4"
    assert to_short_postcode("AA9         4AA") == "AA9 4"
    assert to_short_postcode("") == ""
    assert to_short_postcode("AA9         4AA") == "AA9 4"
    assert to_short_postcode("AA94AA") == "AA9 4"


def test_to_month_only_dob():
    assert to_month_only_dob(datetime.datetime(2020, 5, 17)) == datetime.datetime(
        2020, 5, 1
    )
    assert to_month_only_dob("Non Date Thing") == ""


def test_to_date():
    assert (
        to_date(datetime.datetime(2020, 3, 19)) == datetime.datetime(2020, 3, 19).date()
    )
    assert to_date("15/03/2017") == datetime.datetime(2017, 3, 15).date()


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


class TestCheckYear(unittest.TestCase):
    def test_check_year(self):
        with self.assertRaises(AttributeError):
            check_year("file_no_year.csv")

    def test_check_year_2(self):
        with self.assertRaises(AttributeError):
            check_year("1811.csv")
