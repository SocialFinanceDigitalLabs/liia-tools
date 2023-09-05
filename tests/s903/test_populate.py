import tempfile as tmp

from liiatools.datasets.s903.lds_ssda903_clean import populate

from sfdata_stream_parser import events


def test_add_year_column():
    stream = populate.add_year_column(
        [
            events.StartTable(filename="test_file_20082022"),
            events.EndRow(),
            events.EndTable(),
        ],
        year="2022",
    )
    stream = list(stream)
    assert stream[0].year == "2022"
    assert stream[1].year == "2022"
    assert not hasattr(stream[2], "year")


def test_create_la_child_id():
    stream = populate.create_la_child_id(
        [
            events.Cell(header="CHILD", cell=" 123"),
            events.Cell(header="CHILD", cell=""),
            events.Cell(header="CHILD", cell=None),
            events.Cell(header="NOT_CHILD", cell="456"),
            events.Cell(header="CHILD", cell=789),
            events.Cell(header="CHILD", cell=1011.0),
            events.Cell(header="CHILD", cell="1213.0"),
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
