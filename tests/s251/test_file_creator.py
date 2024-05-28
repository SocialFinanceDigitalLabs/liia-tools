from liiatools.datasets.s251.lds_s251_clean import file_creator

from sfdata_stream_parser import events
from datetime import datetime
import tablib


def test_create_tables():
    la_name = "Barking & Dagenham"
    expected_columns = ["Child ID", "Date of birth"]
    row = [12345, datetime(2019, 4, 15).date()]
    year = 2019
    quarter = "Q1"
    data = tablib.Dataset(headers=expected_columns + ["LA", "Year", "Quarter"])
    data.append(row + [la_name, year, quarter])

    stream = (
        events.StartTable(expected_columns=expected_columns),
        file_creator.RowEvent(row=row, year=year, quarter=quarter),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.data.headers == data.headers
            assert event.data[0] == data[0]

    stream = (
        events.StartTable(expected_columns=expected_columns, match_error="some_error"),
        file_creator.RowEvent(
            row=[12345, datetime(2019, 5, 15).date()], year=year, quarter=quarter
        ),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.data is None

    stream = (
        events.StartTable(expected_columns=expected_columns, year_error="some_error"),
        file_creator.RowEvent(
            row=[12345, datetime(2019, 6, 15).date()], year=year, quarter=quarter
        ),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.data is None
