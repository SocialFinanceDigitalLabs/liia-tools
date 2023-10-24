import pytest

from liiatools.common import converters
from liiatools.common.spec.__data_schema import Column


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
    assert converters.to_category("M1", column) == "M1"
    assert converters.to_category("MF", column) == "MF"

    with pytest.raises(ValueError):
        converters.to_category("M2", column)
    assert converters.to_category("", column) == ""
    assert converters.to_category(None, column) == ""

    column = Column(
        canbeblank=False,
        category=[{"code": "0", "name": "False"}, {"code": "1", "name": "True"}],
    )
    assert converters.to_category(0, column) == "0"
    assert converters.to_category("false", column) == "0"
    assert converters.to_category(1.0, column) == "1"
    assert converters.to_category("true", column) == "1"
    assert converters.to_category("", column) == ""
    assert converters.to_category(None, column) == ""

    with pytest.raises(ValueError):
        converters.to_category("string", column)
    with pytest.raises(ValueError):
        converters.to_category(123, column)

    column = Column(
        canbeblank=False,
        category=[
            {
                "code": "b) Female",
                "name": "F",
                "cell_regex": ["/.*fem.*/i", "/b\).*/i"]
            },
            {
                "code": "a) Male",
                "name": "M",
                "cell_regex": ["/^mal.*/i", "/a\).*/i"]
            },
            {
                "code": "c) Not stated/recorded",
                "name": "Not stated/recorded",
                "cell_regex": ["/not.*/i", "/.*unknown.*/i", "/.*indeterminate.*/i", "/c\).*/i"]
            },
            {
                "code": "d) Neither",
                "name": "Neither",
                "cell_regex": ["/d\).*/i"]
            },
        ],
    )
    assert converters.to_category("b) Female", column) == "b) Female"
    assert converters.to_category("M", column) == "a) Male"
    assert converters.to_category("c)", column) == "c) Not stated/recorded"

    with pytest.raises(ValueError):
        converters.to_category("e)", column)
    assert converters.to_category("", column) == ""
    assert converters.to_category(None, column) == ""
