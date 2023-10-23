import pytest

from liiatools.annex_a_pipeline import converters
from liiatools.annex_a_pipeline.spec import Column


def test_to_category():
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
