import tablib
import functools
import logging
import os

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

log = logging.getLogger(__name__)


class RowEvent(events.ParseEvent):
    pass


def save_stream(stream, la_name, output):
    """
    Outputs stream to file
    :param stream: The stream to output
    :param la_name: Full name of the LA
    :param output: Location to write the output
    :return: Updated stream
    """
    stream = coalesce_row(stream)
    stream = filter_rows(stream)
    stream = create_tables(stream, la_name=la_name)
    stream = save_tables(stream, output=output)
    return stream


def coalesce_row(stream):
    """
    Create a dictionary of the column headers and cell values for a whole row, excluding those with unknown
    column headers
    For duplicate column headers, add a number to the end of the column header, so it can still be saved and
    output in the final file
    :param stream: The stream to output
    :return: Updated stream
    """
    row = None
    duplicate_column = []
    for event in stream:
        if isinstance(event, events.StartRow):
            row = {}
        elif isinstance(event, events.EndRow):
            yield RowEvent.from_event(event, row=row)
            row = None
        elif (
            row is not None
            and isinstance(event, events.Cell)
            and event.column_header != "Unknown"
        ):
            if event.column_header in row:
                duplicate_column.append(event.column_header)
                row[f"{event.column_header} {+len(duplicate_column)+1}"] = event.value
            else:
                row[event.column_header] = event.value
        else:
            yield event


@streamfilter(
    check=type_check(RowEvent), fail_function=pass_event, error_function=pass_event
)
def filter_rows(event):
    """
    Filter out all the rows that contain blank values in columns that need to be populated for data retention
    responsibilities
    :param event: The stream to output
    :return: Updated stream
    """
    # May have to move this somewhere else once I better understand the pattern
    event_types = {
        "List 1": "Date of Contact",
        "List 3": "Date of referral",
        "List 5": "Strategy discussion initiating Section 47 Enquiry Start Date",
        "List 9": "Date of Birth",
        "List 10": "Date of Decision that Child Should be Placed for Adoption",
        "List 11": "Date enquiry received",
    }

    if event.sheet_name in event_types:
        if event.row[event_types[event.sheet_name]]:
            yield event.from_event(event, filter=0)
        else:
            yield event.from_event(event, filter=1)
    else:
        yield event.from_event(event, filter=0)


class TableEvent(events.ParseEvent):
    pass


def create_tables(stream, la_name):
    """
    Append all the rows for a given table to create one concatenated data event for tables with column headers
    that matched the config file
    Ignore any tables that did not have column headers matching the config file
    :param stream: The stream to output
    :param la_name: The name of the local authority
    :return: Updated stream
    """
    data = None
    for event in stream:
        if isinstance(event, events.StartTable):
            try:
                new_headers = event.matched_column_headers + ["LA"]
                data = tablib.Dataset(headers=new_headers)
            except AttributeError:
                data = None
        elif isinstance(event, events.EndTable):
            yield event
            yield TableEvent.from_event(event, data=data)
            data = None
        elif data is not None and isinstance(event, RowEvent):
            if event.filter == 0:
                data.append(list(event.row.values()) + [la_name])
            elif event.filter == 1:
                yield event
        yield event


def save_tables(stream, output):
    """
    Save the data events as Excel files in the output directory as long as there are exactly 11 sheets
    :param stream: The stream to output
    :param output: The location of the output file
    :return: updated stream object.
    """
    book = tablib.Databook()
    sheet_name = ""
    for event in stream:
        if isinstance(event, events.StartContainer):
            book = tablib.Databook()
        elif isinstance(event, events.EndContainer) and len(book.sheets()) == 11:
            with open(f"{os.path.join(output, event.filename)}_clean.xlsx", "wb") as f:
                f.write(book.export("xlsx"))
        elif isinstance(event, events.StartTable):
            try:
                sheet_name = event.sheet_name
            except AttributeError:
                sheet_name = ""
        elif isinstance(event, TableEvent) and event.data is not None:
            dataset = event.data
            dataset.title = sheet_name
            book.add_sheet(dataset)
        yield event


@functools.cache
def lookup_column_config(table_name, column_name):
    return None
