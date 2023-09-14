import logging

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

log = logging.getLogger(__name__)


class ErrorTable(events.ParseEvent):
    pass


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def blank_error_check(event):
    """
    Check all the values against the config to see if they are allowed to be blank
    if they are blank but should not be, record this as event.blank_error = 1

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    try:
        allowed_blank = event.config_dict["canbeblank"]
        formatting_error = getattr(event, "formatting_error", "0")
        below_zero_error = getattr(event, "below_zero_error", "0")
        if (
            not allowed_blank
            and (event.cell == "" or event.cell is None)
            and formatting_error != "1"
            and below_zero_error != "1"
        ):
            return event.from_event(event, blank_error="1")
        else:
            return event
    except AttributeError:  # Raised in case there is no config item for the given cell
        pass


def create_blank_error_list(stream):
    """
    Create a list of the column headers for cells with blank fields that should not be blank (event.blank_error = 1)
    for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with blank error lists
    """
    blank_error_list = None
    for event in stream:
        if isinstance(event, events.StartTable):
            blank_error_list = []
        elif isinstance(event, events.EndTable):
            blank_error_list = None
        elif isinstance(event, ErrorTable):
            yield ErrorTable.from_event(event, blank_error_list=blank_error_list)
            blank_error_list = None
        elif blank_error_list is not None and isinstance(event, events.Cell):
            blank_error = getattr(event, "blank_error", "0")
            if blank_error == "1":
                blank_error_list.append(event.header)
            else:
                pass
        yield event


def create_below_zero_error_list(stream):
    """
    Create a list of the column headers for cells with fields below zero for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with blank error lists
    """
    below_zero_error_list = None
    for event in stream:
        if isinstance(event, events.StartTable):
            below_zero_error_list = []
        elif isinstance(event, events.EndTable):
            below_zero_error_list = None
        elif isinstance(event, ErrorTable):
            yield ErrorTable.from_event(
                event, below_zero_error_list=below_zero_error_list
            )
            below_zero_error_list = None
        elif below_zero_error_list is not None and isinstance(event, events.Cell):
            below_zero_error = getattr(event, "below_zero_error", "0")
            if below_zero_error == "1":
                below_zero_error_list.append(event.header)
            else:
                pass
        yield event
