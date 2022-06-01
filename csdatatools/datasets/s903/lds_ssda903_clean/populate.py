import re
import logging

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event)
def add_year_column(event):
    """
    Searches the filename for the year by finding any four-digit number starting with 20
    """
    try:
        file_dir = event.filename
        match = re.search(r"20\d{2}", file_dir)
        year = match.group(0)
        return event.from_event(event, year=year)
    except AttributeError:
        log.exception(f"Unable to find year in {event.filename}")
        return event.from_event(event, year_error=f"Unable to find year in {event.filename}")
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")


@streamfilter(check=lambda x: x.get("header") in ["CHILD"], fail_function=pass_event)
def create_la_child_id(event, config, la_name):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps
    """
    try:
        la_child_id = f"{config[la_name]}_{event.cell}"
        yield event.from_event(event, cell=la_child_id)
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")
