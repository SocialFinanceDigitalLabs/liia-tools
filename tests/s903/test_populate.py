from liiatools.datasets.s903.lds_ssda903_clean import populate

from sfdata_stream_parser import events


def test_add_year_column():
    stream = populate.add_year_column(
        [
            events.StartTable(filename="test_file_2022"),
            events.EndRow(),
            events.EndTable(),
            events.StartTable(filename="test_file_no_year"),
            events.EndRow(),
        ]
    )
    stream = list(stream)
    assert stream[0].year == "2022"
    assert stream[1].year == "2022"
    assert not hasattr(stream[2], "year")
    assert stream[3].year_error == f"Unable to find year in {stream[3].filename} so no output has been produced"
    assert not hasattr(stream[4], "year")


def test_create_la_child_id():
    stream = populate.create_la_child_id(
        [
            events.Cell(header="CHILD", cell="123"),
            events.Cell(header="NOT_CHILD", cell="456"),
        ],
        la_code="BAD",
    )
    stream = list(stream)
    assert stream[0].cell == "123_BAD"
    assert stream[1].cell == "456"
