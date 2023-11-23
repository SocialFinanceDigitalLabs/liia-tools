import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.social_work_workforce.lds_csww_clean.converters import (
    to_category,
    to_numeric,
    to_regex,
)

from liiatools.datasets.shared_functions.converters import to_date

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the schema xsd file

    :param event: A filtered list of event objects of type TextNode
    :return: An updated list of event objects
    """
    dateformat = event.schema_dict["date"]
    try:
        clean_text = to_date(event.text, dateformat)
        return event.from_event(event, text=clean_text, formatting_error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", formatting_error="1")


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the schema xsd file

    :param event: A filtered list of event objects of type TextNode
    :return: An updated list of event objects
    """
    category = event.schema_dict["category"]
    try:
        clean_text = to_category(event.text, category)
        if clean_text != "error":
            return event.from_event(event, text=clean_text, formatting_error="0")
        return event.from_event(event, text="", formatting_error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", formatting_error="1")


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def clean_numeric(event):
    """
    Convert all values that should be integers or decimals to integers or decimals based on the schema xsd file

    :param event: A filtered list of event objects of type TextNode
    :return: An updated list of event objects
    """
    numeric = event.schema_dict["numeric"]
    try:
        decimal_places = event.schema_dict.get("decimal", None)
        min_inclusive = event.schema_dict.get("min_inclusive", None)
        max_inclusive = event.schema_dict.get("max_inclusive", None)
        clean_text = to_numeric(
            value=event.text,
            config=numeric,
            decimal_places=decimal_places,
            min_inclusive=min_inclusive,
            max_inclusive=max_inclusive,
        )
        if clean_text != "error":
            return event.from_event(event, text=clean_text, formatting_error="0")
        return event.from_event(event, text="", formatting_error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", formatting_error="1")


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def clean_regex_string(event):
    """
    Convert all values that should be regex strings to regex strings based on the schema xsd file

    :param event: A filtered list of event objects of type TextNode
    :return: An updated list of event objects
    """
    pattern = event.schema_dict["regex_string"]
    try:
        clean_text = to_regex(event.text, pattern)
        if clean_text != "error":
            return event.from_event(event, text=clean_text, formatting_error="0")
        return event.from_event(event, text="", formatting_error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", formatting_error="1")


def clean(stream):
    """
    Compile the cleaning functions

    :param stream: A list of event objects
    :return: An updated list of event objects
    """
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    stream = clean_numeric(stream)
    stream = clean_regex_string(stream)
    return stream
