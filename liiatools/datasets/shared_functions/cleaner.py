import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.shared_functions.converters import (
    to_date,
    to_category,
    to_integer,
)

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    date = event.config_dict["date"]
    try:
        text = to_date(event.cell, date)
        return event.from_event(event, cell=text, formatting_error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", formatting_error="1")


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    category = event.config_dict["category"]
    try:
        text = to_category(event.cell, category)
        if text != "formatting_error":
            return event.from_event(event, cell=text, formatting_error="0")
        else:
            return event.from_event(event, cell="", formatting_error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", formatting_error="1")


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_integers(event):
    """
    Convert all values that should be integers into integers based on the config file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    numeric = event.config_dict["numeric"]
    try:
        text = to_integer(event.cell, numeric)
        if text == "value_below_zero":
            return event.from_event(event, cell="", below_zero_error="1")
        else:
            return event.from_event(event, cell=text, formatting_error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", formatting_error="1")
