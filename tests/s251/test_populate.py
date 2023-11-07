from liiatools.datasets.s251.lds_s251_clean import populate

from sfdata_stream_parser import events


def test_add_year_column():
    stream = populate.add_year_column(
        [
            events.StartTable(),
            events.EndRow(),
            events.EndTable(),
        ],
        year="2022",
        quarter="Q1",
    )
    stream = list(stream)
    assert not hasattr(stream[0], "year") and not hasattr(stream[0], "quarter")
    assert stream[1].year == "2022" and stream[1].quarter == "Q1"
    assert not hasattr(stream[2], "year") and not hasattr(stream[0], "quarter")


def test_create_la_child_id():
    stream = populate.create_la_child_id(
        [
            events.Cell(header="Child ID", cell=" 123"),
            events.Cell(header="Child ID", cell=""),
            events.Cell(header="Child ID", cell=None),
            events.Cell(header="Test", cell="456"),
            events.Cell(header="Child ID", cell=789),
            events.Cell(header="Child ID", cell=1011.0),
            events.Cell(header="Child ID", cell="1213.0"),
        ],
        la_code="BAD",
    )
    stream = list(stream)
    assert stream[0].cell == "123_BAD"
    assert stream[1].cell == "_BAD"
    assert stream[2].cell == "None_BAD"
    assert stream[3].cell == "456"
    assert stream[4].cell == "789_BAD"
    assert stream[5].cell == "1011_BAD"
    assert stream[6].cell == "1213_BAD"
