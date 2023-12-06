import pytest
import datetime

from liiatools.common.converters import (
    allow_blank,
    to_category,
    _match_postcode,
    to_postcode,
    to_short_postcode,
    _check_range,
    to_numeric,
    to_date,
    to_nth_of_month,
    to_regex,
)
from liiatools.common.spec.__data_schema import Column


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


def test_to_category():
    column = Column(
        canbeblank=False,
        category=[
            {"code": "M1"},
            {"code": "F1"},
            {"code": "MM"},
            {"code": "FF"},
            {"code": "MF"},
        ],
    )
    assert to_category("M1", column) == "M1"
    assert to_category("MF", column) == "MF"

    with pytest.raises(ValueError):
        to_category("M2", column)
    assert to_category("", column) == ""
    assert to_category(None, column) == ""

    column = Column(
        canbeblank=False,
        category=[{"code": "0", "name": "False"}, {"code": "1", "name": "True"}],
    )
    assert to_category(0, column) == "0"
    assert to_category("false", column) == "0"
    assert to_category(1.0, column) == "1"
    assert to_category("true", column) == "1"
    assert to_category("", column) == ""
    assert to_category(None, column) == ""

    with pytest.raises(ValueError):
        to_category("string", column)
    with pytest.raises(ValueError):
        to_category(123, column)

    column = Column(
        canbeblank=False,
        category=[
            {
                "code": "b) Female",
                "name": "F",
                "cell_regex": ["/.*fem.*/i", "/b\).*/i"],
            },
            {"code": "a) Male", "name": "M", "cell_regex": ["/^mal.*/i", "/a\).*/i"]},
            {
                "code": "c) Not stated/recorded",
                "name": "Not stated/recorded",
                "cell_regex": [
                    "/not.*/i",
                    "/.*unknown.*/i",
                    "/.*indeterminate.*/i",
                    "/c\).*/i",
                ],
            },
            {"code": "d) Neither", "name": "Neither", "cell_regex": ["/d\).*/i"]},
        ],
    )
    assert to_category("b) Female", column) == "b) Female"
    assert to_category("M", column) == "a) Male"
    assert to_category("c)", column) == "c) Not stated/recorded"

    with pytest.raises(ValueError):
        to_category("e)", column)
    assert to_category("", column) == ""
    assert to_category(None, column) == ""


def test_to_postcode():
    assert to_postcode("AA9 4AA") == "AA9 4AA"
    assert to_postcode("   AA9 4AA   ") == "AA9 4AA"
    assert to_postcode("") == ""
    assert to_postcode(None) == ""
    assert to_postcode("AA9         4AA") == "AA9 4AA"
    assert to_postcode("AA94AA") == "AA9 4AA"
    assert to_postcode("A A 9 4 A A") == "AA9 4AA"

    with pytest.raises(ValueError):
        to_postcode("ABCD 1234")

    with pytest.raises(ValueError):
        to_postcode(345)

    with pytest.raises(ValueError):
        to_postcode({})


def test_to_short_postcode():
    assert to_short_postcode("AA9 4AA") == "AA9 4"
    assert to_short_postcode("   AA9 4AA   ") == "AA9 4"
    assert to_short_postcode("AA9         4AA") == "AA9 4"
    assert to_short_postcode("") == ""
    assert to_short_postcode("AA9         4AA") == "AA9 4"
    assert to_short_postcode("AA94AA") == "AA9 4"

    with pytest.raises(ValueError):
        to_short_postcode("ABCD 1234")


def test_to_numeric():
    assert to_numeric("3000", "integer") == 3000
    assert to_numeric(123, "integer") == 123
    assert to_numeric("", "integer") == ""
    assert to_numeric(None, "integer") == ""
    assert to_numeric("1.0", "integer") == 1
    assert to_numeric(0, "integer") == 0

    assert to_numeric(1.23, "float") == 1.23
    assert to_numeric("1.23", "float") == 1.23
    assert to_numeric("", "float") == ""
    assert to_numeric(None, "float") == ""
    assert to_numeric(0.5, "float", min_value=0, max_value=1) == 0.5
    assert to_numeric(0.2, "float", min_value=0) == 0.2
    assert to_numeric(0.1234, "float", decimal_places=3) == 0.123

    with pytest.raises(ValueError):
        to_numeric("date", "integer")
        to_numeric(1.5, "float", min_value=0, max_value=1)
        to_numeric(1.5, "float", min_value=2)


def test_to_date():
    assert (
        to_date(datetime.datetime(2020, 3, 19)) == datetime.datetime(2020, 3, 19).date()
    )
    assert to_date("15/03/2017") == datetime.datetime(2017, 3, 15).date()


def test_to_nth_of_month():
    assert to_nth_of_month(datetime.datetime(2020, 5, 17)) == datetime.datetime(
        2020, 5, 1
    )

    with pytest.raises(ValueError):
        to_nth_of_month("Non Date Thing")


def test_to_regex():
    pattern = r"[A-Za-z]{2}\d{10}"
    assert to_regex("AB1234567890", pattern) == "AB1234567890"
    assert to_regex("  AB1234567890  ", pattern) == "AB1234567890"
    assert to_regex("", pattern) == ""
    assert to_regex(None, pattern) == ""

    with pytest.raises(ValueError):
        to_regex("AB1234567890123456", pattern)
        to_regex("AB12345", pattern)
        to_regex("xxxxOz2054309383", pattern)
