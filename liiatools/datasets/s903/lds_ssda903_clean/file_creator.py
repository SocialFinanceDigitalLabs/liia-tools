import functools
import tablib
import logging
import os

from sfdata_stream_parser import events

log = logging.getLogger(__name__)


class RowEvent(events.ParseEvent):
    pass


def coalesce_row(stream):
    """
    Create a list of the cell values for a whole row
    """
    row = None
    for event in stream:
        if isinstance(event, events.StartRow):
            row = []
        elif isinstance(event, events.EndRow):
            yield RowEvent.from_event(event, row=row)
            row = None
        elif row is not None and isinstance(event, events.Cell):
            row.append(event.cell)
        else:
            yield event


class TableEvent(events.ParseEvent):
    pass


def create_tables(stream, la_name):
    """
    Append all the rows for a given table to create one concatenated data event
    """
    data = None
    for event in stream:
        if isinstance(event, events.StartTable):
            try:
                if event.match_error is not None:
                    data = None
            except AttributeError:
                data = tablib.Dataset(headers=event.headers + ["LA", "YEAR"])
        elif isinstance(event, events.EndTable):
            yield event
            yield TableEvent.from_event(event, data=data)
            data = None
        elif data is not None and isinstance(event, RowEvent):
            try:
                data.append(event.row + [la_name, event.year])
            except AttributeError:  # raised in case event.year is missing so data is not added
                pass
        yield event


def save_tables(stream, output):
    """
    Save the data events as csv files in the Outputs directory
    """
    for event in stream:
        if isinstance(event, TableEvent) and event.data is not None:
            dataset = event.data
            with open(
                f"{os.path.join(output, event.filename)}_clean.csv", "w", newline=""
            ) as f:
                f.write(dataset.export("csv"))
        yield event


@functools.cache
def lookup_column_config(table_name, column_name):
    return None
