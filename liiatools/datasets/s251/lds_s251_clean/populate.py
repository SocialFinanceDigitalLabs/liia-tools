import logging

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


def add_year_column(stream, year, quarter):
    """
    Attaches the year of the file to the events.StartTable and events.EndRow

    :param stream: A filtered list of event objects of type StartTable
    :param year: Year of the file
    :param quarter: Quarter of the file
    :return: An updated list of event objects
    """
    for event in stream:
        if isinstance(event, events.StartTable):
            yield event
        elif isinstance(event, events.EndTable):
            yield event
            year = None
        elif isinstance(event, events.EndRow) and year is not None:
            yield event.from_event(event, year=year, quarter=quarter)
        else:
            yield event


@streamfilter(check=lambda x: x.get("header") in ["Child ID"], fail_function=pass_event)
def create_la_child_id(event, la_code):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps

    :param event: A filtered list of event objects
    :param la_code: The 3-character LA code used to identify a local authority
    :return: An updated list of event objects
    """
    if isinstance(event.cell, str) and event.cell[-2:] == ".0":
        child_id = int(float(event.cell))
    elif isinstance(event.cell, str):
        child_id = event.cell.strip()
    elif isinstance(event.cell, float):
        child_id = int(event.cell)
    else:
        child_id = event.cell

    la_child_id = f"{child_id}_{la_code}"
    yield event.from_event(event, cell=la_child_id)
