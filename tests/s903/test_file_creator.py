from liiatools.datasets.s903.lds_ssda903_clean import file_creator

from sfdata_stream_parser import events
from datetime import datetime
import tablib


def test_coalesce_row():
    stream = (
        events.StartRow(),
        events.Cell(cell="value_one"),
        events.Cell(cell="value_two"),
        events.EndRow(),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.as_dict()["row"] == ["value_one", "value_two"]

    stream = (
        events.StartRow(),
        events.Cell(cell=125),
        events.Cell(cell=341),
        events.EndRow(),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.as_dict()["row"] == [125, 341]

    stream = (
        events.StartRow(),
        events.Cell(cell=125),
        events.Cell(cell="string"),
        events.Cell(cell=datetime(2020, 3, 23)),
        events.EndRow(),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.as_dict()["row"] == [
        125,
        "string",
        datetime(2020, 3, 23),
    ]

    stream = (
        events.StartRow(),
        events.Cell(cell=125),
        events.Cell(cell=None),
        events.EndRow(),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.as_dict()["row"] == [125, None]

    stream = (
        events.StartRow(),
        events.Cell(cell=125),
        events.Cell(cell=""),
        events.EndRow(),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))[0]
    assert events_complete_rows.as_dict()["row"] == [125, ""]

    stream = (
        events.StartTable(),
        events.StartRow(),
        events.Cell(cell="value_one"),
        events.Cell(cell="value_two"),
        events.EndRow(),
        events.EndTable(),
    )
    events_complete_rows = list(file_creator.coalesce_row(stream))
    for event in events_complete_rows:
        if isinstance(event, file_creator.RowEvent):
            assert event.as_dict()["row"] == ["value_one", "value_two"]
        else:
            assert event.as_dict() == {}


def test_create_tables():
    la_name = "Barking & Dagenham"

    stream = (
        events.StartTable(headers=["CHILD ID", "DOB"]),
        file_creator.RowEvent(row=[12345, datetime(2019, 4, 15).date()], year=2019),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            data = tablib.Dataset(headers=["CHILD ID", "DOB", "LA", "YEAR"])
            data.append([12345, datetime(2019, 4, 15).date(), la_name, 2019])
            assert event.as_dict()["data"].headers == data.headers
            assert event.as_dict()["data"][0] == data[0]

    stream = (
        events.StartTable(headers=["CHILD ID", "DOB"], match_error="some_error"),
        file_creator.RowEvent(row=[12345, datetime(2019, 4, 15).date()], year=2019),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            assert event.as_dict()["data"] is None

    stream = (
        events.StartTable(headers=["CHILD ID", "DOB", "DATE_INT"]),
        file_creator.RowEvent(row=[12345, None, ""], year=2019),
        events.EndTable(),
    )
    events_with_tables = list(file_creator.create_tables(stream, la_name=la_name))
    for event in events_with_tables:
        if isinstance(event, file_creator.TableEvent):
            data = tablib.Dataset(headers=["CHILD ID", "DOB", "DATE_INT", "LA", "YEAR"])
            data.append([12345, None, "", la_name, 2019])
            assert event.as_dict()["data"].headers == data.headers
            assert event.as_dict()["data"][0] == data[0]


# def test_save_tables():
# file_creator.save_tables()
