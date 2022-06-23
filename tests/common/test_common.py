from liiatools.datasets.shared_functions.common import (
    to_short_postcode,
    check_postcode,
    flip_dict,
    to_month_only_dob,
)
import datetime


def test_flip_dict():
    assert flip_dict({"key": "value"}) == {"value": "key"}


def test_check_postcode():
    assert check_postcode("AA9 4AA") == "AA9 4AA"
    assert check_postcode("   AA9 4AA   ") == "AA9 4AA"
    assert check_postcode("") == ""
    assert check_postcode("AA9         4AA") == "AA9         4AA"
    assert check_postcode("AA94AA") == "AA94AA"
    assert check_postcode("123456") == ""
    assert check_postcode("AA54aEG33587") == ""


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
