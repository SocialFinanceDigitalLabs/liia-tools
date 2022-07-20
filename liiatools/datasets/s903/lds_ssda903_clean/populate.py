import re
import logging

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


def add_year_column(stream):
    """
    Searches the filename for the year by finding any four-digit number starting with 20

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
    year = None
    for event in stream:
        if isinstance(event, events.StartTable):
            try:
                file_dir = event.filename
                match = re.search(r"20\d{2}", file_dir)
                year = match.group(0)
                yield event.from_event(event, year=year)
            except AttributeError:
                year_error = f"Unable to find year in '{event.filename}' so no output has been produced"
                yield event.from_event(event, year_error=year_error)
        elif isinstance(event, events.EndTable):
            yield event
            year = None
        elif isinstance(event, events.EndRow) and year is not None:
            yield event.from_event(event, year=year)
        else:
            yield event


@streamfilter(check=lambda x: x.get("header") in ["CHILD"], fail_function=pass_event)
def create_la_child_id(event, la_code):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps

    :param event: A filtered list of event objects
    :param la_code: The 3-character LA code used to identify a local authority
    :return: An updated list of event objects
    """
    la_child_id = f"{event.cell}_{la_code}"
    yield event.from_event(event, cell=la_child_id)
