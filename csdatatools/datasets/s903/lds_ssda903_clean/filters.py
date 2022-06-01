import logging

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

from csdatatools.datasets.s903.lds_ssda903_clean.converters import to_category, to_integer, to_date
from csdatatools.datasets.shared_functions.common import check_postcode

log = logging.getLogger(__name__)


@streamfilter(check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file
    """
    date = event.config_dict["date"]
    try:
        text = to_date(event.cell, date)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")


@streamfilter(check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config.yaml file
    """
    category = event.config_dict["category"]
    try:
        text = to_category(event.cell, category)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")


@streamfilter(check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event)
def clean_integers(event):
    """
    Convert all values that should be integers into integers based on the config.yaml file
    """
    numeric = event.config_dict["numeric"]
    try:
        text = to_integer(event.cell, numeric)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")


@streamfilter(check=lambda x: x.get('header') in ['HOME_POST', 'PL_POST'], fail_function=pass_event)
def clean_postcodes(event):
    """
    Check that all values that should be postcodes are postcodes
    """
    try:
        text = check_postcode(event.cell)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")


def clean(stream):
    """
    Compile the cleaning functions
    """
    stream = clean_dates(stream)
    stream = clean_categories(stream)
    stream = clean_integers(stream)
    stream = clean_postcodes(stream)
    return stream
