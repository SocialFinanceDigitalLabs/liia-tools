import logging

from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


@streamfilter(check=lambda x: x.get("column_header") in ["Child Unique ID", "Individual adopter identifier"],
              fail_function=pass_event)
def create_la_child_id(event, config, la_name):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps
    """
    try:
        la_child_id = f"{event.value}_{config[la_name]}"
        yield event.from_event(event, value=la_child_id)
    except Exception as ex:
        log.exception(f"Error occurred in {event.filename}")
