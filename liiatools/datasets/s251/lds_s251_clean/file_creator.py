import tablib
import logging

from sfdata_stream_parser import events

from liiatools.datasets.shared_functions.file_creator import (
    coalesce_row,
    save_tables,
    TableEvent,
    RowEvent,
)

log = logging.getLogger(__name__)


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
            if match_error is not None or year_error is not None:
                data = None
            else:
                data = tablib.Dataset(
                    headers=event.expected_columns + ["LA", "Year", "Quarter"]
                )
        elif isinstance(event, events.EndTable):
            yield event
            yield TableEvent.from_event(event, data=data)
            data = None
        elif data is not None and isinstance(event, RowEvent):
            try:
                data.append(event.row + [la_name, event.year, event.quarter])
            except AttributeError:  # raised in case event.year is missing so data is not added
                pass
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
