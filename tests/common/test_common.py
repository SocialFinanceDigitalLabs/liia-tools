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
    assert check_year("file_2022.csv") == "2022"
    assert check_year("file_140032021.csv") == "2021"
    assert check_year("file_2017-18.csv") == "2018"
    assert check_year("file_201819.csv") == "2019"
