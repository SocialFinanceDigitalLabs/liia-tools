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


def coalesce_row(stream):
    """
    Create a list of the cell values for a whole row excluding those with unknown column headers
    """
    row = None
    for event in stream:
        if isinstance(event, events.StartRow):
            row = {}
        elif isinstance(event, events.EndRow):
            yield RowEvent.from_event(event, row=row)
            row = None
        elif row is not None and isinstance(event, events.Cell) and event.column_header != "Unknown":
            row[event.column_header] = event.value
        else:
            yield event


@streamfilter(check=type_check(RowEvent), fail_function=pass_event, error_function=pass_event)
def filter_rows(event):
    """
    Filter out all the rows that contain blank values in columns that need to be populated for data retention
    responsibilities
    """
    # May have to move this somewhere else once I better understand the pattern
    event_types = {
        "List 1": "Date of Contact",
        "List 3": "Date of referral",
        "List 5": "Strategy discussion initiating Section 47 Enquiry Start Date",
        "List 9": "Date of Birth",
        "List 10": "Date of Decision that Child Should be Placed for Adoption",
        "List 11": "Date enquiry received"
    }

    if event.sheet_name in event_types and event.row[event_types[event.sheet_name]] == "":
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
    """
    data = None
    for event in stream:
        if isinstance(event, events.StartTable):
            try:
                new_headers = event.matched_column_headers.add("LA")
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
    Save the data events as Excel files in the London Datastore/Cleaned files directory
    """
    book = tablib.Databook()
    for event in stream:
        if isinstance(event, events.StartContainer):
            book = tablib.Databook()
        elif isinstance(event, events.EndContainer):
            with open(f"{os.path.join(output, event.filename)}_clean.xlsx", "wb") as f:
                f.write(book.export("xlsx"))
        elif isinstance(event, TableEvent) and event.data is not None:
            dataset = event.data
            book.add_sheet(dataset)
        yield event


@functools.cache
def lookup_column_config(table_name, column_name):
    return None
