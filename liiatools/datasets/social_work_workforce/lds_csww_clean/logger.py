import logging
from datetime import datetime
from pathlib import Path

from sfdata_stream_parser.filters.generic import streamfilter, events

log = logging.getLogger(__name__)


def inherit_LAchildID(stream):
    """
    Apply the LAchildID to all elements within <Child></Child>

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    child_id = child_events = None
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == "Child":
            child_events = [event]
        elif isinstance(event, events.EndElement) and event.tag == "Child":
            child_events.append(event)
            yield from [e.from_event(e, LAchildID=child_id) for e in child_events]
            child_id = child_events = None
        elif isinstance(event, events.TextNode):
            try:
                child_events.append(event)
                if event.schema.name == "LAchildID":
                    child_id = event.text
            except AttributeError:  # Raised in case there is no LAchildID or event is None
                pass
        elif child_events:
            child_events.append(event)
        else:
            yield event


@streamfilter(check=lambda x: True)
def counter(event, counter_check, value_error, structural_error, blank_error):
    """
    Count the invalid simple nodes storing their name and LAchildID data

    :param event: A filtered list of event objects
    :param counter_check: A function to identify which events to check
    :param value_error: An empty list to store the invalid element information
    :param structural_error: An empty list to store the invalid structure information
    :param blank_error: An empty list to store the blank element information
    :return: The same filtered list of event objects
    """
    if counter_check(event) and len(event.node) == 0:
        if (
            getattr(event, "LAchildID", None) is not None
        ):  # In case there are errors in the <Header> node as none
            # of these will have an LAchildID assigned
            if hasattr(event, "validation_message"):
                blank_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.schema.name}"
                )
            elif hasattr(event.schema, "name"):
                value_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.schema.name}"
                )
            else:
                structural_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.tag}"
                )
        else:
            if hasattr(event, "validation_message"):
                blank_error.append(
                    f"LAchildID: blank, Node: {event.schema.name}"
                )
            elif hasattr(event.schema, "name"):
                value_error.append(f"LAchildID: blank, Node: {event.schema.name}")
            else:
                structural_error.append(f"LAchildID: blank, Node: {event.tag}")
    return event


def save_errors_la(
    input,
    value_error,
    structural_error,
    blank_error,
    LAchildID_error,
    field_error,
    la_log_dir,
):
    """
    Save the errors as a text file in the Local Authority Logs directory
    only save the errors if there is at least one

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param value_error: A list of invalid element information
    :param structural_error: A list of invalid structure information
    :param field_error: A list of missing fields
    :param blank_error: A list of blank fields that should have contained data
    :param LAchildID_error: A list of missing LAchildID information
    :param la_log_dir: Location to save the gathered error logs
    :return: An updated list of event objects
    """
    filename = str(Path(input).resolve().stem)
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"
    if (
        value_error
        or structural_error
        or field_error
        or blank_error
        or LAchildID_error
    ):
        with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
        ) as f:
            f.write(filename)
            f.write("\n")
            if value_error:
                f.write(
                    "Node information for data that has been removed because it was found to be invalid"
                )
                f.write("\n")
                for item in value_error:
                    f.write(item)
                    f.write("\n")
                f.write("\n")
            if structural_error:
                f.write(
                    "Node information for fields that has been removed because they did not match the "
                    "expected structure"
                )
                f.write("\n")
                for item in structural_error:
                    f.write(item)
                    f.write("\n")
                f.write("\n")
            if blank_error:
                f.write(
                    "Node information for blank fields that should have contained data"
                )
                f.write("\n")
                for item in blank_error:
                    f.write(item)
                    f.write("\n")
                f.write("\n")
            if LAchildID_error:
                LAchildID_error = list(
                    dict.fromkeys(LAchildID_error)
                )  # Remove duplicate information from list but
                # keep order
                for item in LAchildID_error:
                    f.write(item)
                    f.write("\n")
            if field_error:
                field_error = list(
                    dict.fromkeys(field_error)
                )  # Remove duplicate information from list but keep order
                for item in field_error:
                    f.write(item)
                    f.write("\n")
                f.write("\n")
