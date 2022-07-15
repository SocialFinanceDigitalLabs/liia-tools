import logging
from datetime import datetime
from pathlib import Path
import uuid

from sfdata_stream_parser.filters.generic import streamfilter, events

log = logging.getLogger(__name__)


def create_child_uuid(stream):
    """
    Adds a universally unique ID to each Child element so LAchildID can be attached to a whole Child

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    unique_id = None
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == "Child":
            unique_id = uuid.uuid1()
        elif isinstance(event, events.EndElement) and event.tag == "Child":
            unique_id = None

        if unique_id and not hasattr(event, "unique_id"):
            event = event.from_event(event, **{"unique_id": unique_id})

        yield event


def create_uuid_to_LAchildID_map(stream, map_dict):
    """
    Create a map of the unique_id and LAchildID

    :param stream: A filtered list of event objects
    :param map_dict: An empty dictionary to hold the mapping information
    :return: An updated list of event objects
    """
    for event in stream:
        try:
            if isinstance(event, events.TextNode) and event.schema.name == "LAchildID":
                child_id = getattr(event, "text", None)
                map_dict[event.unique_id] = child_id
                yield event

        except AttributeError:
            yield event

        yield event


def map_uuid_to_LAchildID(stream, map_dict):
    """
    Maps LAchildID to each element based on its unique_id

    :param stream: A filtered list of event objects
    :param map_dict: An dictionary holding the mapping information
    :return: An updated list of event objects
    """
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == "Child":
            try:
                event = event.from_event(event, **{"LAchildID": map_dict[event.unique_id]})
                yield event
            except (KeyError, AttributeError) as e:
                print(map_dict, "----", e)
                yield event



    # child_id = None
    # for event in stream:
    #     try:
    #         if isinstance(event, events.StartElement) and event.tag == "Child":
    #             child_id = child_id
    #         elif isinstance(event, events.EndElement) and event.tag == "Child":
    #             child_id = None
    #
    #         if isinstance(event, events.TextNode) and event.schema.name == "LAchildID":
    #             child_id = getattr(event, "text", None)
    #
    #         if child_id and not hasattr(event, "LAchildID"):
    #             event = event.from_event(event, **{"LAchildID": child_id})
    #
    #         yield event
    #
    #     except AttributeError:  # In case event does not contain schema.name so information can be written to
    #         # a log for the Local Authority
    #         yield event


@streamfilter(check=lambda x: True)
def counter(event, counter_check, value_error, structural_error, LAchildID_error):
    """
    Count the invalid simple nodes storing their name and LAchildID data

    :param event: A filtered list of event objects
    :param counter_check: A function to identify which events to check
    :param value_error: An empty list to store the invalid element information
    :param structural_error: An empty list to store the invalid structure information
    :return: The same filtered list of event objects
    """
    if counter_check(event) and len(event.node) == 0:
        try:
            value_error.append(
                f"LAchildID: {event.LAchildID}, Node: {event.schema.name}"
            )
        except AttributeError:  # Raised in case there is no event.schema.name
            try:
                structural_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.tag}"
                )
            except AttributeError:  # Raised in case there is no LAchildID
                return event
    return event


def save_errors_la(
    input, value_error, structural_error, LAchildID_error, field_error, la_log_dir
):
    """
    Save the errors as a text file in the Local Authority Logs directory
    only save the errors if there is at least one

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param value_error: A list of invalid element information
    :param structural_error: A list of invalid structure information
    :param field_error: A list of missing fields
    :param la_log_dir: Location to save the gathered error logs
    :return: An updated list of event objects
    """
    filename = str(Path(input).resolve().stem)
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    if value_error or structural_error or field_error or LAchildID_error:
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
