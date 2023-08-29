from liiatools.datasets.s903.lds_ssda903_clean import converters
from liiatools.datasets.s903.spec import Column


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
    assert converters.to_category("M2", column) == None
    assert converters.to_category("MF", column) == "MF"
    assert converters.to_category("", column) == ""
    # assert converters.to_category(None, column) == ""

    column = Column(
        canbeblank=False,
        category=[{"code": "0", "name": "False"}, {"code": "1", "name": "True"}],
    )
    assert converters.to_category(0, column) == "0"
    assert converters.to_category("false", column) == "0"
    assert converters.to_category(1.0, column) == "1"
    assert converters.to_category("true", column) == "1"
    assert converters.to_category("string", column) == "error"
    assert converters.to_category(123, column) == None
    # assert converters.to_category("", column) == ""
    # assert converters.to_category(None, column) == ""


def test_to_integer():
    assert converters.to_integer("3000", "integer") == 3000
    assert converters.to_integer(123, "integer") == 123
    assert converters.to_integer("date", "") == "date"
    assert converters.to_integer("", "integer") == ""
    assert converters.to_integer(None, "integer") == ""
    assert converters.to_integer("1.0", "integer") == 1
    assert converters.to_integer(0, "integer") == 0
