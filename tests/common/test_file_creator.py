from liiatools.datasets.shared_functions import file_creator

from sfdata_stream_parser import events
from datetime import datetime
import tablib
import tempfile as tmp
from unittest.mock import patch
from pathlib import Path


def test_coalesce_row():
    stream = (
        events.StartRow(expected_columns=["Header_1", "Header_2"]),
        events.Cell(
            cell="value_one",
            header="Header_1",
            expected_columns=["Header_1", "Header_2"],
        ),
        events.Cell(
            cell="value_two",
            header="Header_2",
            expected_columns=["Header_1", "Header_2"],
        ),
        events.EndRow(expected_columns=["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == ["value_one", "value_two"]

    stream = (
        events.StartRow(expected_columns=["Header_1", "Header_2"]),
        events.Cell(
            cell=125, header="Header_1", expected_columns=["Header_1", "Header_2"]
        ),
        events.Cell(
            cell=341, header="Header_2", expected_columns=["Header_1", "Header_2"]
        ),
        events.EndRow(year=2019, expected_columns=["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [125, 341]
    assert events_complete_rows.year == 2019

    stream = (
        events.StartRow(expected_columns=["Header_1", "Header_2", "Header_3"]),
        events.Cell(
            cell=125,
            header="Header_1",
            expected_columns=["Header_1", "Header_2", "Header_3"],
        ),
        events.Cell(
            cell="string",
            header="Header_2",
            expected_columns=["Header_1", "Header_2", "Header_3"],
        ),
        events.Cell(
            cell=datetime(2020, 3, 23),
            header="Header_3",
            expected_columns=["Header_1", "Header_2", "Header_3"],
        ),
        events.EndRow(expected_columns=["Header_1", "Header_2", "Header_3"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [
        125,
        "string",
        datetime(2020, 3, 23),
    ]

    stream = (
        events.StartRow(expected_columns=["Header_1", "Header_2"]),
        events.Cell(
            cell=125, header="Header_1", expected_columns=["Header_1", "Header_2"]
        ),
        events.Cell(
            cell=None, header="Header_2", expected_columns=["Header_1", "Header_2"]
        ),
        events.EndRow(expected_columns=["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [125, None]

    stream = (
        events.StartRow(expected_columns=["Header_1", "Header_2"]),
        events.Cell(
            cell=125, header="Header_1", expected_columns=["Header_1", "Header_2"]
        ),
        events.Cell(
            cell="", header="Header_2", expected_columns=["Header_1", "Header_2"]
        ),
        events.EndRow(expected_columns=["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [125, ""]

    stream = (
        events.StartTable(expected_columns=["Header_1", "Header_2"]),
        events.StartRow(expected_columns=["Header_1", "Header_2"]),
        events.Cell(
            cell="value_one",
            header="Header_1",
            expected_columns=["Header_1", "Header_2"],
        ),
        events.Cell(
            cell="value_two",
            header="Header_2",
            expected_columns=["Header_1", "Header_2"],
        ),
        events.EndRow(expected_columns=["Header_1", "Header_2"]),
        events.EndTable(expected_columns=["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))
    for event in events_complete_rows:
        if isinstance(event, file_creator.RowEvent):
            assert event.row == ["value_one", "value_two"]
        else:
            assert event.as_dict() == {"expected_columns": ["Header_1", "Header_2"]}


@patch("builtins.open", create=True)
def test_save_tables(mock_save):
    data = tablib.Dataset(headers=["Name", "Age"])
    data.append(("Kenneth", 12))
    output = tmp.gettempdir()

    stream = file_creator.save_tables(
        [file_creator.TableEvent(filename="test_file", data=data)], output
    )
    list(stream)

    mock_save.assert_called_once_with(
        f"{Path(output, 'test_file')}_clean.csv", "w", newline=""
    )
    # mock_save.write.assert_called_once_with(data.export("csv"))
