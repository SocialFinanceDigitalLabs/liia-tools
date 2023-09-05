import logging

from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.shared_functions.common import check_postcode
from liiatools.datasets.shared_functions.cleaner import (
    clean_dates,
    clean_integers,
    clean_categories,
)

log = logging.getLogger(__name__)


@streamfilter(
    check=lambda x: x.get("header") in ["HOME_POST", "PL_POST"],
    fail_function=pass_event,
)
def clean_postcodes(event):
    """
    Check that all values that should be postcodes are postcodes

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    error = "0"
    text = ""
    try:
        text = check_postcode(event.cell)
    except (AttributeError, TypeError, ValueError):
        error = "1"
    return event.from_event(event, cell=text, formatting_error=error)


def clean(stream):
    """
    Compile the cleaning functions
    """
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    stream = clean_integers(stream)
    stream = clean_postcodes(stream)
    return stream
