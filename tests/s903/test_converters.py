import pytest

from liiatools.ssda903_pipeline.lds_ssda903_clean import converters
from liiatools.ssda903_pipeline.spec import Column


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
        converters.to_category("string", column) == None
    with pytest.raises(ValueError):
        converters.to_category(123, column) == None
