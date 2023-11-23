import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.shared_functions.common import check_postcode
from liiatools.datasets.shared_functions.cleaner import (
    clean_dates,
    clean_categories,
    clean_integers,
)

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.Cell),
    fail_function=pass_event,
    error_function=pass_event,
)
def clean_postcodes(event):
    """
    Check that all values that should be postcodes are postcodes based on the config file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    postcode = event.config_dict["string"]
    if postcode == "postcode":
        try:
            text = check_postcode(event.cell)
            return event.from_event(event, cell=text, formatting_error="0")
        except (AttributeError, TypeError, ValueError):
            return event.from_event(event, cell="", formatting_error="1")
    else:
        return event


def clean(stream):
    """
    Compile the cleaning functions
    """
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    stream = clean_integers(stream)
    stream = clean_postcodes(stream)
    return stream
