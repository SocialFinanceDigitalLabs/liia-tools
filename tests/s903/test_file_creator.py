from liiatools.datasets.s903.lds_ssda903_clean import file_creator

from sfdata_stream_parser import events
from datetime import datetime
import tablib
import tempfile as tmp
from unittest.mock import patch
from pathlib import Path


def test_coalesce_row():
    stream = (
        events.StartRow(expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell="value_one", header="Header_1", expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell="value_two", header="Header_2", expected_columns = ["Header_1", "Header_2"]),
        events.EndRow(expected_columns = ["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == ["value_one", "value_two"]

    stream = (
        events.StartRow(expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell=125, header="Header_1", expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell=341, header="Header_2", expected_columns = ["Header_1", "Header_2"]),
        events.EndRow(year=2019, expected_columns = ["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [125, 341]
    assert events_complete_rows.year == 2019

    stream = (
        events.StartRow(expected_columns = ["Header_1", "Header_2", "Header_3"]),
        events.Cell(cell=125, header="Header_1", expected_columns = ["Header_1", "Header_2", "Header_3"]),
        events.Cell(cell="string", header="Header_2", expected_columns = ["Header_1", "Header_2", "Header_3"]),
        events.Cell(cell=datetime(2020, 3, 23), header="Header_3", expected_columns = ["Header_1", "Header_2", "Header_3"]),
        events.EndRow(expected_columns = ["Header_1", "Header_2", "Header_3"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [
        125,
        "string",
        datetime(2020, 3, 23),
    ]

    stream = (
        events.StartRow(expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell=125, header="Header_1", expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell=None, header="Header_2", expected_columns = ["Header_1", "Header_2"]),
        events.EndRow(expected_columns = ["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [125, None]

    stream = (
        events.StartRow(expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell=125, header="Header_1", expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell="", header="Header_2", expected_columns = ["Header_1", "Header_2"]),
        events.EndRow(expected_columns = ["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.row == [125, ""]

    stream = (
        events.StartTable(expected_columns = ["Header_1", "Header_2"]),
        events.StartRow(expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell="value_one", header="Header_1", expected_columns = ["Header_1", "Header_2"]),
        events.Cell(cell="value_two", header="Header_2", expected_columns = ["Header_1", "Header_2"]),
        events.EndRow(expected_columns = ["Header_1", "Header_2"]),
        events.EndTable(expected_columns = ["Header_1", "Header_2"]),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))
    for event in events_complete_rows:
        if isinstance(event, file_creator.RowEvent):
            assert event.row == ["value_one", "value_two"]
        else:
            assert event.as_dict() == {"expected_columns":["Header_1", "Header_2"]}


def test_create_tables():
    la_name = "Barking & Dagenham"
    headers = ["CHILD ID", "DOB"]
    expected_columns = ["CHILD ID", "DOB"]
    row = [12345, datetime(2019, 4, 15).date()]
    year = 2019
    data = tablib.Dataset(headers=headers + ["LA", "YEAR"])
    data.append(row + [la_name, year])

    stream = (
        events.StartTable(headers=headers, expected_columns=expected_columns),
        file_creator.RowEvent(row=row, year=year),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.data.headers == data.headers
            assert event.data[0] == data[0]

    stream = (
        events.StartTable(headers=["CHILD ID", "DOB"], expected_columns=expected_columns, match_error="some_error"),
        file_creator.RowEvent(row=[12345, datetime(2019, 4, 15).date()], year=2019),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.data is None

    headers = ["CHILD ID", "DOB", "DATE_INT"]
    expected_columns = ["CHILD ID", "DOB", "DATE_INT"]
    row = [12345, None, ""]
    year = 2019
    data = tablib.Dataset(headers=expected_columns + ["LA", "YEAR"])
    data.append(row + [la_name, year])

    stream = (
        events.StartTable(headers=headers, expected_columns=expected_columns),
        file_creator.RowEvent(row=row, year=year),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.data.headers == data.headers
            assert event.data[0] == data[0]


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
