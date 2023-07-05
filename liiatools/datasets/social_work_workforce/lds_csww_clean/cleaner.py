import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from liiatools.datasets.s903.lds_ssda903_clean.converters import to_category, to_integer

from liiatools.datasets.shared_functions.converters import to_date
from liiatools.datasets.shared_functions.common import check_postcode

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.TextNode), fail_function=pass_event, error_function=pass_event
)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    date = event.config_dict["date"]
    try:
        text = to_date(event.cell, date)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")
