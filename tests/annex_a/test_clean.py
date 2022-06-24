from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean.cleaner import clean_integers, clean_cell_category


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
                value=0
            ),
        ]
    )
    stream = list(stream)
    assert stream[0].value == 5
    assert stream[0].error == "0"
    assert stream[1].value == ""
    assert stream[1].error == "0"
    assert stream[2].value == ""
    assert stream[2].error == "1"
    assert stream[3].value == "3"
    assert stream[4].value == ""
    assert stream[4].error == "1"
    assert stream[5].value == 0
    assert stream[5].error == "0"


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
                ]
            ),
            events.Cell(
                value="random_value",
                category_config=[
                    {
                        "code": "a) WBRI",
                        "name": "White British",
                        "regex": ["/.*whi.*british.*/i", "/a\\).*/i", "/WBRI.*/i"],
                    }
                ]
            ),
            events.Cell(
                value="random_value"
            )
        ]
    )
    stream = list(stream)
    assert stream[0].value == "b) Female"
    assert stream[0].error == "0"
    assert stream[1].value == "a) WBRI"
    assert stream[1].error == "0"
    assert stream[2].value == ""
    assert stream[2].error == "0"
    assert stream[3].value == ""
    assert stream[3].error == "1"
    assert stream[4].value == "random_value"
