import datetime
import unittest

import pytest

from liiatools.datasets.shared_functions.common import (
    check_postcode,
    check_year,
    check_year_within_range,
)
from liiatools.datasets.shared_functions.converters import (
    allow_blank,
    to_date,
    to_integer,
    to_month_only_dob,
    to_short_postcode,
)


def test_allow_blank():
    @allow_blank
    def test_function(_):
        return "CALLED"

    assert test_function(None) == ""
    assert test_function("") == ""
    assert test_function("    \r\n   ") == ""

    assert test_function("TEST") == "CALLED"

    with pytest.raises(ValueError):
        test_function(None, allow_blank=False)

    with pytest.raises(ValueError):
        test_function("", allow_blank=False)

    with pytest.raises(ValueError):
        test_function("    \r\n   ", allow_blank=False)


def test_check_postcode():
    assert check_postcode("AA9 4AA") == "AA9 4AA"
    assert check_postcode("   AA9 4AA   ") == "AA9 4AA"
    assert check_postcode("") == ""
    assert check_postcode(None) == ""
    assert check_postcode("AA9         4AA") == "AA9 4AA"
    assert check_postcode("AA94AA") == "AA9 4AA"
    assert check_postcode("A A 9 4 A A") == "AA9 4AA"

    with pytest.raises(ValueError):
        check_postcode("ABCD 1234")

    with pytest.raises(ValueError):
        check_postcode(345)

    with pytest.raises(ValueError):
        check_postcode({})


def test_to_short_postcode():
    assert to_short_postcode("AA9 4AA") == "AA9 4"
    assert to_short_postcode("   AA9 4AA   ") == "AA9 4"
    assert to_short_postcode("AA9         4AA") == "AA9 4"
    assert to_short_postcode("") == ""
    assert to_short_postcode("AA9         4AA") == "AA9 4"
    assert to_short_postcode("AA94AA") == "AA9 4"

    with pytest.raises(ValueError):
        to_short_postcode("ABCD 1234")


def test_to_month_only_dob():
    assert to_month_only_dob(datetime.datetime(2020, 5, 17)) == datetime.datetime(
        2020, 5, 1
    )

    with pytest.raises(ValueError):
        to_month_only_dob("Non Date Thing")


def test_to_date():
    assert (
        to_date(datetime.datetime(2020, 3, 19)) == datetime.datetime(2020, 3, 19).date()
    )
    assert to_date("15/03/2017") == datetime.datetime(2017, 3, 15).date()


def test_to_integer():
    assert to_integer("3000") == 3000
    assert to_integer(123) == 123
    assert to_integer("") == ""
    assert to_integer(None) == ""
    assert to_integer("1.0") == 1
    assert to_integer(0) == 0

    with pytest.raises(ValueError):
        to_integer("date")


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


def test_check_year_within_range():
    assert check_year_within_range(2016, 6, 6, datetime.datetime(2023, 5, 31)) is False
    assert check_year_within_range(2023, 6, 6, datetime.datetime(2023, 5, 31)) is True
    assert check_year_within_range(2024, 6, 6, datetime.datetime(2023, 5, 31)) is False
    assert check_year_within_range(2024, 6, 6, datetime.datetime(2023, 6, 1)) is True
    assert check_year_within_range(2013, 10, 2, datetime.datetime(2023, 1, 31)) is True


class TestCheckYear(unittest.TestCase):
    def test_check_year(self):
        with self.assertRaises(ValueError):
            check_year("file_no_year.csv")

    def test_check_year_2(self):
        with self.assertRaises(ValueError):
            check_year("1811.csv")
