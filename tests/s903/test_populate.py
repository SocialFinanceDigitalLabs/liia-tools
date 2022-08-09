import tempfile as tmp
import unittest

from liiatools.datasets.s903.lds_ssda903_clean import populate

from sfdata_stream_parser import events


def test_add_year_column():
    la_log_dir = tmp.gettempdir()
    input = r"test_file_2022.csv"

    stream = populate.add_year_column(
        [
            events.StartTable(filename="test_file_2022"),
            events.EndRow(),
            events.EndTable(),
        ],
        input=input,
        la_log_dir=la_log_dir
    )
    stream = list(stream)
    assert stream[0].year == "2022"
    assert stream[1].year == "2022"
    assert not hasattr(stream[2], "year")


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
