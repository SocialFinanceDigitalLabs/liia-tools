from datetime import datetime, date

from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean.cleaner import (
    clean_integers,
    clean_cell_category,
    clean_dates,
    clean_postcodes,
)


def test_clean_integers():
    stream = clean_integers(
        [
            events.Cell(
                column_index=0,
                category_config={"canbeblank": False},
                other_config={"type": "integer"},
                value="5",
            ),
            events.Cell(
                column_index=1,
                category_config={"canbeblank": False},
                other_config={"type": "integer"},
                value="",
            ),
            events.Cell(
                column_index=2,
                category_config={"canbeblank": False},
                other_config={"type": "integer"},
                value="a",
            ),
            events.Cell(
                column_index=3,
                category_config={"canbeblank": False},
                other_config={},
                value="3",
            ),
            events.Cell(
                column_index=4,
                category_config={"canbeblank": False},
                other_config={"type": "integer"},
            ),
            events.Cell(
                column_index=5,
                category_config={"canbeblank": False},
                other_config={"type": "integer"},
                value=0,
            ),
        ]
    )
    stream = list(stream)
    assert stream[0].value == 5
    assert stream[0].formatting_error == "0"
    assert stream[1].value == ""
    assert stream[1].formatting_error == "0"
    assert stream[2].value == ""
    assert stream[2].formatting_error == "1"
    assert stream[3].value == "3"
    assert stream[4].value == ""
    assert stream[4].formatting_error == "1"
    assert stream[5].value == 0
    assert stream[5].formatting_error == "0"


def test_clean_cell_category():
    stream = clean_cell_category(
        [
            events.Cell(
                value="fem",
                category_config=[
                    {
                        "code": "b) Female",
                        "name": "F",
                        "regex": ["/.*fem.*/i", "/b\\).*/i"],
                    },
                    {
                        "code": "a) Male",
                        "name": "M",
                        "regex": ["/^mal.*/i", "/a\\).*/i"],
                    },
                ],
            ),
            events.Cell(
                value="white british",
                category_config=[
                    {
                        "code": "a) WBRI",
                        "name": "White British",
                        "regex": ["/.*whi.*british.*/i", "/a\\).*/i", "/WBRI.*/i"],
                    }
                ],
            ),
            events.Cell(
                value="",
                category_config=[
                    {
                        "code": "a) WBRI",
                        "name": "White British",
                        "regex": ["/.*whi.*british.*/i", "/a\\).*/i", "/WBRI.*/i"],
                    }
                ],
            ),
            events.Cell(
                value="random_value",
                category_config=[
                    {
                        "code": "a) WBRI",
                        "name": "White British",
                        "regex": ["/.*whi.*british.*/i", "/a\\).*/i", "/WBRI.*/i"],
                    }
                ],
            ),
            events.Cell(value="random_value"),
            events.Cell(
                value="Monkey",
                category_config=[
                    {
                        "code": "a) Male",
                        "name": "M",
                        "regex": ["/^mal.*/i", "/a\\).*/i"],
                    },
                ],
            ),
        ]
    )
    stream = list(stream)
    assert stream[0].value == "b) Female"
    assert stream[0].formatting_error == "0"
    assert stream[1].value == "a) WBRI"
    assert stream[1].formatting_error == "0"
    assert stream[2].value == ""
    assert stream[2].formatting_error == "0"
    assert stream[3].value == ""
    assert stream[3].formatting_error == "1"
    assert stream[4].value == "random_value"
    assert stream[5].value == ""
    assert stream[5].error == "1"


def test_clean_dates():
    stream = clean_dates(
        [
            events.Cell(value=datetime(2012, 2, 21), other_config={"type": "date"}),
            events.Cell(value=date(2013, 3, 23), other_config={"type": "date"}),
            events.Cell(value="15/2/2021", other_config={"type": "date"}),
            events.Cell(value="not_date", other_config={"type": "date"}),
            events.Cell(value="", other_config={"type": "date"}),
            events.Cell(value=None, other_config={"type": "date"}),
            events.Cell(value="random_value", other_config={"type": "not_a_date"}),
        ]
    )
    stream = list(stream)
    assert stream[0].value == date(2012, 2, 21)
    assert stream[0].error == "0"
    assert stream[1].value == date(2013, 3, 23)
    assert stream[1].error == "0"
    assert stream[2].value == date(2021, 2, 15)
    assert stream[2].error == "0"
    assert stream[3].value == ""
    assert stream[3].error == "1"
    assert stream[4].value == ""
    assert stream[4].error == "0"
    assert stream[5].value == ""
    assert stream[5].error == "0"
    assert stream[6].value == "random_value"
    assert not hasattr(stream[6], "error")


def test_clean_postcodes():
    stream = clean_postcodes(
        [
            events.Cell(value="SW19 3XL", column_header="Placement postcode"),
            events.Cell(value="   SW19 3XL   ", column_header="Placement postcode"),
            events.Cell(value="xxSW19 3XLzz", column_header="Placement postcode"),
            events.Cell(value="", column_header="Placement postcode"),
            events.Cell(value=None, column_header="Placement postcode"),
            events.Cell(value="al1 3zv", column_header="Placement postcode"),
            events.Cell(value="random_value", column_header="not a Placement postcode"),
        ]
    )
    stream = list(stream)
    assert stream[0].value == "SW19 3XL"
    assert stream[0].error == "0"
    assert stream[1].value == "SW19 3XL"
    assert stream[1].error == "0"
    assert stream[2].value == ""
    assert stream[2].error == "1"
    assert stream[3].value == ""
    assert stream[3].error == "0"
    assert stream[4].value == ""
    assert stream[4].error == "0"
    assert stream[5].value == "al1 3zv"
    assert stream[5].error == "0"
    assert stream[6].value == "random_value"
    assert not hasattr(stream[6], "error")
    assert stream[5].formatting_error == "1"
