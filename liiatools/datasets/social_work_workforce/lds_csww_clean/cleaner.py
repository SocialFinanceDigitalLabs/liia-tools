import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.social_work_workforce.lds_csww_clean.converters import to_category, to_integer, to_decimal, to_regex

from liiatools.datasets.shared_functions.converters import to_date
#from liiatools.datasets.shared_functions.common import check_postcode

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
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
        return event.from_event(event, text=clean_text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
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
            return event.from_event(event, text=clean_text, error='0')
        return event.from_event(event, text="", error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
)
def clean_numeric(event):
    """
    Convert all values that should be integers or decimals to integers or decimals based on the schema xsd file

    :param event: A filtered list of event objects of type TextNode
    :return: An updated list of event objects
    """
    numeric = event.schema_dict["numeric"]
    try:
        if numeric == "integer":
            clean_text = to_integer(event.text, numeric)
        elif numeric == "decimal":
            decimal_places = int(event.schema_dict["decimal"])
            clean_text = to_decimal(event.text, numeric, decimal_places)
        if clean_text != "error":
            return event.from_event(event, text=clean_text, error='0')
        return event.from_event(event, text="", error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
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
            return event.from_event(event, text=clean_text, error="0")
        return event.from_event(event, text="", error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")