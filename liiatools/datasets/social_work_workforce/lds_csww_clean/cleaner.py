import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.social_work_workforce.lds_csww_clean.converters import to_category, to_integer, to_decimal

from liiatools.datasets.shared_functions.converters import to_date
#from liiatools.datasets.shared_functions.common import check_postcode

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the schema xsd file

    :param event: A filtered list of event objects of type text
    :return: An updated list of event objects
    """
    dateformat = event.schema_dict["date"]
    try:
        newtext = to_date(event.text, dateformat)
        return event.from_event(event, text=newtext, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the schema xsd file

    :param event: A filtered list of event objects of type text
    :return: An updated list of event objects
    """
    category = event.schema_dict["category"]
    try:
        newtext = to_category(event.text, category)
        if newtext != "error":
            return event.from_event(event, text=f"*cat*{newtext}", error='0')
        return event.from_event(event, text="", error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
)
def clean_numeric(event):
    """
    Convert all values that should be integers to integers based on the schema xsd file

    :param event: A filtered list of event objects of type integer
    :return: An updated list of event objects
    """
    numeric = event.schema_dict["numeric"]
    #print(integer)
    try:
        if numeric == "integer":
            newtext = to_integer(event.text, numeric)
        elif numeric == "decimal":
            newtext = to_decimal(event.text, numeric)
        if newtext != "error":
            return event.from_event(event, text=f"*{numeric[:3]}*{newtext}", error='0')
        return event.from_event(event, text="", error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, text="", error="1")


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
)   
def clean_regex_string(event):
    """
    Convert all values that should be regex strings to regex strings based on the schema xsd file

    :param event: A filtered list of event objects of type regex string
    :return: An updated list of event objects
    """
    pattern = event.schema_dict["regex_string"]
    print(pattern)