import logging

from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


@streamfilter(
    check=lambda x: x.get("column_header")
    in ["Child Unique ID", "Individual adopter identifier"],
    fail_function=pass_event,
)
def create_la_child_id(event, la_code):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps

    :param event: A filtered list of event objects
    :param la_code: The 3-character LA code used to identify a local authority
    :return: An updated list of event objects
    """
    la_child_id = f"{event.value}_{la_code}"
    yield event.from_event(event, value=la_child_id)
