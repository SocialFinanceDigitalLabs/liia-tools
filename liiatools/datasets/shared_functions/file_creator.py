import logging
from pathlib import Path

from sfdata_stream_parser import events

log = logging.getLogger(__name__)


class RowEvent(events.ParseEvent):
    pass


class TableEvent(events.ParseEvent):
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
        elif row is not None and isinstance(event, events.Cell):
            expected_columns = getattr(event, "expected_columns", ())
            if event.header in set(expected_columns):
                row.append(event.cell)
        else:
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
                f"{Path(output, event.filename)}_clean.csv", "w", newline=""
            ) as f:
                f.write(dataset.export("csv"))
        yield event
