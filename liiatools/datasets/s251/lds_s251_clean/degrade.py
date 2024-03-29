import logging

from sfdata_stream_parser.filters.generic import (
    streamfilter,
    pass_event,
    type_check,
    events,
)

from liiatools.datasets.shared_functions.converters import (
    to_short_postcode,
    to_month_only_dob,
)

log = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.Cell),
    fail_function=pass_event,
    error_function=pass_event,
)
def degrade_postcodes(event):
    """
    Convert all values that should be postcodes to shorter postcodes

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    postcode = event.config_dict["string"]
    if postcode == "postcode":
        text = to_short_postcode(event.cell)
        return event.from_event(event, cell=text)
    else:
        return event


@streamfilter(
    check=lambda x: x.get("header") in ["Date of birth"], fail_function=pass_event
)
def degrade_dob(event):
    """
    Convert all values that should be dates of birth to months and year of birth

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    text = to_month_only_dob(event.cell)
    return event.from_event(event, cell=text)


def degrade(stream):
    """
    Compile the degrading functions
    """
    stream = degrade_postcodes(stream)
    stream = degrade_dob(stream)
    return stream
