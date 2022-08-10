from liiatools.datasets.annex_a.lds_annexa_clean import populate

from sfdata_stream_parser import events


def test_create_la_child_id():
    stream = populate.create_la_child_id(
        [
            events.Cell(column_header="Child Unique ID", value="123"),
            events.Cell(column_header="Individual adopter identifier", value="456"),
            events.Cell(column_header="not a child", value="789"),
        ],
        la_code="BAD",
    )
    stream = list(stream)
    assert stream[0].value == "123_BAD"
    assert stream[1].value == "456_BAD"
    assert stream[2].value == "789"
