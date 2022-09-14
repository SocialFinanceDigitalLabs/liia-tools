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

    :param stream: The stream to output
    :return: Updated stream
    """
    row = None
    for event in stream:
        if isinstance(event, events.StartRow):
            row = []
        elif isinstance(event, events.EndRow):
            yield RowEvent.from_event(event, row=row)
            row = None
        elif (
            row is not None
            and isinstance(event, events.Cell)
            and event.header in set(event.expected_columns)
        ):
            row.append(event.cell)
        else:
            yield event


class TableEvent(events.ParseEvent):
    pass


def create_tables(stream, la_name):
    """
    Append all the rows for a given table to create one concatenated data event

    :param stream: The stream to output
    :param la_name: The name of the local authority
    :return: Updated stream
    """
    data = None
    for event in stream:
        if isinstance(event, events.StartTable):
            match_error = getattr(event, "match_error", None)
            year_error = getattr(event, "year_error", None)
            if match_error or year_error is not None:
                data = None
            else:
                data = tablib.Dataset(headers=event.expected_columns + ["LA", "YEAR"])
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

    :param stream: The stream to output
    :param output: The location of the output file
    :return: updated stream object.
    """
    for event in stream:
        if isinstance(event, TableEvent) and event.data is not None:
            dataset = event.data
            with open(
                f"{os.path.join(output, event.filename)}_clean.csv", "w", newline=""
            ) as f:
                f.write(dataset.export("csv"))
        yield event


def save_stream(stream, la_name, output):
    """
    Outputs stream to file

    :param stream: The stream to output
    :param la_name: Full name of the LA
    :param output: Location to write the output
    :return: Updated stream
    """
    stream = coalesce_row(stream)
    stream = create_tables(stream, la_name=la_name)
    stream = save_tables(stream, output=output)
    return stream


@functools.cache
def lookup_column_config(table_name, column_name):
    return None
