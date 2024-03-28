import logging

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

from liiatools.datasets.annex_a.lds_annexa_clean.converters import to_integer
from liiatools.datasets.shared_functions.common import check_postcode
from liiatools.datasets.shared_functions.converters import to_date
from liiatools.datasets.annex_a.lds_annexa_clean.regex import parse_regex

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_cell_category(event):
    """
    Checks the values of a cell against the config file using code, name and regex matching
    returns the matched value for a successful match and blank if not
    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """

    try:
        for c in event.category_config:
            if event.value:
                if str(c["code"]).lower() in str(event.value).lower():
                    return event.from_event(
                        event, value=c["code"], formatting_error="0"
                    )
                elif str(c["name"]).lower() == str(event.value).lower():
                    return event.from_event(
                        event, value=c["code"], formatting_error="0"
                    )

                for r in c.get("regex", []):
                    p = parse_regex(r)
                    if p.match(str(event.value)) is not None:
                        return event.from_event(
                            event, value=c["code"], formatting_error="0"
                        )

            else:
                return event.from_event(event, value="", formatting_error="0")

        else:
            return event.from_event(event, value="", formatting_error="1")
    except (
        AttributeError,
        KeyError,
    ):  # Raised in case there is no config item for the given cell
        return event


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_integers(event):
    """
    Convert all values that should be integers into integers based on the annex-a-merge.yaml file
    if they cannot be converted record this as event.error = 1
    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    try:
        numeric = event.other_config["type"]
    except (
        AttributeError,
        KeyError,
    ):  # Raised in case there is no config item for the given cell
        return event

    if numeric == "integer":
        try:
            text = to_integer(event.value)
            return event.from_event(event, value=text, formatting_error="0")
        except (AttributeError, TypeError, ValueError):
            return event.from_event(event, value="", formatting_error="1")
    else:
        return event


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the annex-a-merge.yaml file
    if they cannot be converted record this as event.error = 1
    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    try:
        date = event.other_config["type"]
    except (
        AttributeError,
        KeyError,
    ):  # Raised in case there is no config item for the given cell
        return event
    if date == "date":
        try:
            text = to_date(event.value)
            return event.from_event(event, value=text, formatting_error="0")
        except (AttributeError, TypeError, ValueError):
            return event.from_event(event, value="", formatting_error="1")
    else:
        return event


@streamfilter(
    check=lambda x: x.get("column_header") in ["Placement postcode"],
    fail_function=pass_event,
)
def clean_postcodes(event):
    """
    Check that all values that should be postcodes are postcodes
    if they are not postcodes record this as event.error = 1
    :param event: A filtered list of event objects which have a column header of "Placement postcode"
    :return: An updated list of event objects
    """
    error = "0"
    text = ""
    try:
        text = check_postcode(event.value)
    except (AttributeError, TypeError, ValueError):
        error = "1"
    return event.from_event(event, value=text, formatting_error=error)


def clean(stream):
    """
    Compile cleaning functions
    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    stream = clean_cell_category(stream)
    stream = clean_integers(stream)
    stream = clean_dates(stream)
    stream = clean_postcodes(stream)
    return stream
