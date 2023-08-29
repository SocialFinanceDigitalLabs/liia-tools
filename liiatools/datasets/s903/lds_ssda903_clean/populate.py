import logging
import re

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import pass_event, streamfilter

from liiatools.datasets.shared_functions import common

log = logging.getLogger(__name__)


@streamfilter(check=lambda x: x.get("header") in ["CHILD"], fail_function=pass_event)
def create_la_child_id(event, la_code):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps

    :param event: A filtered list of event objects
    :param la_code: The 3-character LA code used to identify a local authority
    :return: An updated list of event objects
    """
    if isinstance(event.cell, str) and event.cell[-2:] == ".0":
        child_id = int(float(event.cell))
    elif isinstance(event.cell, str):
        child_id = event.cell.strip()
    elif isinstance(event.cell, float):
        child_id = int(event.cell)
    else:
        child_id = event.cell

    la_child_id = f"{child_id}_{la_code}"
    yield event.from_event(event, cell=la_child_id)
