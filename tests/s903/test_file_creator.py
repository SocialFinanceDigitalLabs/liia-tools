from liiatools.datasets.s903.lds_ssda903_clean import file_creator

from sfdata_stream_parser import events
from datetime import datetime
import tablib


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
        events.StartTable(
            headers=["CHILD ID", "DOB"],
            expected_columns=expected_columns,
            match_error="some_error",
        ),
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
