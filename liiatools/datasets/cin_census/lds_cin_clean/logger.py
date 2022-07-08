import logging
from datetime import datetime
from pathlib import Path

from sfdata_stream_parser.filters.generic import streamfilter, events

log = logging.getLogger(__name__)


def inherit_child_id(stream):
    """
    Reads the LAchildID from a TextNode and applies that value to all other events

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    child_id = None
    for event in stream:
        try:
            if isinstance(event, events.TextNode) and event.schema.name == "LAchildID":
                child_id = getattr(event, "text", None)

            if child_id and not hasattr(event, "LAchildID"):
                event = event.from_event(event, **{"LAchildID": child_id})

            yield event
        except AttributeError:  # In case event does not contain schema.name so information can be written to
            # a log for the Local Authority
            yield event


@streamfilter(check=lambda x: True)
def counter(event, counter_check, value_error, structural_error):
    """
    Count the invalid simple nodes storing their name and LAchildID data

    :param event: A filtered list of event objects
    :param counter_check: A function to identify which events to check
    :param value_error: An empty list to store the invalid element information
    :param structural_error: An empty list to store the invalid structure information
    :return: The same filtered list of event objects
    """
    if counter_check(event) and not event.node:
        try:
            value_error.append(
                f"LAchildID: {event.LAchildID}, Node: {event.schema.name}"
            )
        except AttributeError:
            structural_error.append(f"LAchildID: {event.LAchildID}, Node: {event.tag}")
    return event


def save_errors_la(input, value_error, structural_error, la_log_dir):
    """
    Save the errors as a text file in the Local Authority Logs directory
    only save the errors if there is at least one

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param value_error: A list of invalid element information
    :param structural_error: A list of invalid structure information
    :param la_log_dir: Location to save the gathered error logs
    :return: An updated list of event objects
    """
    filename = str(Path(input).resolve().stem)
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    if value_error or structural_error:
        with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
        ) as f:
            f.write(filename)
            f.write("\n")
            if value_error:
                f.write(
                    "LAchildID and node information for data that has been removed because"
                    " it was found to be invalid"
                )
                f.write("\n")
                for item in value_error:
                    f.write(item)
                    f.write("\n")
                f.write("\n")
            if structural_error:
                f.write(
                    "LAchildID and node information for fields that has been removed because they did"
                    " not match the expected structure"
                )
                f.write("\n")
                for item in structural_error:
                    f.write(item)
                    f.write("\n")
                f.write("\n")
