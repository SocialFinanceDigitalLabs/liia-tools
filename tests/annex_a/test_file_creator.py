from datetime import datetime

from liiatools.datasets.annex_a.lds_annexa_clean import (
    file_creator
)


def test_filter_rows():
    stream = file_creator.filter_rows(
        [
            file_creator.RowEvent(sheet_name="List 1", row={"Date of Contact": "some value"}),
            file_creator.RowEvent(sheet_name="List 1", row={"Date of Contact": ""}),
            file_creator.RowEvent(sheet_name="List 1", row={"Date of Contact": None}),
        ]
    )
    stream = list(stream)
    assert stream[0].filter == 0
    assert stream[1].filter == 1
    assert stream[2].filter == 1

