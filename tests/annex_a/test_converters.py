from datetime import datetime

from liiatools.datasets.annex_a.lds_annexa_clean.convertors import (
    to_integer
)

from liiatools.datasets.shared_functions.converters import ( to_date )

def test_conversion_to_integer():
    assert to_integer("1") == 1
    assert to_integer("-1") == -1
    assert to_integer("1.4") == ""
    assert to_integer("") == ""
    assert to_integer("a") == ""


def test_conversion_to_date():
    assert to_date("25/05/2022") == datetime(2022, 5, 25).date()
    assert to_date(datetime(2022, 5, 25)) == datetime(2022, 5, 25).date()
    assert to_date("25/21/2022") == ""
    assert to_date("abcd") == ""
    assert to_date("") == ""