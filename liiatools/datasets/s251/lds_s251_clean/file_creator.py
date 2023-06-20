import tablib
import logging
from pathlib import Path

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
        ):
            row.append(event.cell)
        else:
            yield event


class TableEvent(events.ParseEvent):
    pass


def create_tables(stream):
    """
    Append all the rows for a given table to create one concatenated data event

    :param stream: The stream to output
    :return: Updated stream
    """
    data = None
    for event in stream:
        if isinstance(event, events.StartTable):
            data = tablib.Dataset(headers=event.expected_columns)
        elif isinstance(event, events.EndTable):
            yield event
            yield TableEvent.from_event(event, data=data)
            data = None
        elif data is not None and isinstance(event, RowEvent):
            data.append(event.row)
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
                f"{Path(output)}\\s251.csv", "w", newline=""
            ) as f:
                f.write(dataset.export("csv"))
        yield event


def save_stream(stream, output):
    """
    Outputs stream to file

    :param stream: The stream to output
    :param output: Location to write the output
    :return: Updated stream
    """
    stream = coalesce_row(stream)
    stream = create_tables(stream)
    stream = save_tables(stream, output=output)
    return stream
