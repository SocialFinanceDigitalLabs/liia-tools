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
        if not allowed_blank and not event.cell and formatting_error != "1":
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


@streamfilter(
    check=type_check(events.StartTable),
    fail_function=pass_event,
    error_function=pass_event,
)
def create_file_match_error(event):
    """
    Add a match_error to StartTables that do not have an event.expected_columns so these errors can be written to the
    log.txt file. If there is no event.expected_columns for a given StartTable that means its headers did not match
    those in the config file

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
    expected_columns = getattr(event, "expected_columns", None)
    if expected_columns is None:
        return event.from_event(
            event,
            match_error=f"Failed to find a set of matching columns headers for file titled '{event.filename}' "
            f"which contains column headers {event.headers} so no output has been produced",
        )
    else:
        return event
