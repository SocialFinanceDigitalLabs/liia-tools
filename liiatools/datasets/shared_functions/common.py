import re
import logging
from pathlib import Path
from datetime import datetime

from sfdata_stream_parser import events

log = logging.getLogger(__name__)


def flip_dict(some_dict):
    """
    Potentially a temporary function which switches keys and values in a dictionary.
    May need to rewrite LA-codes YAML file to avoid this step

    :param some_dict: A config dictionary
    :return: a reversed dictionary with keys as values and vice versa
    """
    return {value: key for key, value in some_dict.items()}


def check_postcode(postcode):
    """
    Checks that the postcodes are in the right format
    :param postcode: A string with a UK-style post code
    :return: a post code, or if incorrect a blank string
    """
    if postcode:
        match = re.search(
            r"^[A-Z]{1,2}\d[A-Z\d]? *\d[A-Z]{2}$", postcode.strip(), re.IGNORECASE
        )
        return match.group(0)
    return ""


def to_short_postcode(postcode):
    """
    Remove whitespace from the beginning and end of postcodes and the last two digits for anonymity
    return blank if not in the right format
    :param postcode: A string with a UK-style post code
    :return: a shortened post code with the area, district, and sector. The units is removed
    """
    if postcode:
        try:
            match = re.search(
                r"^[A-Z]{1,2}\d[A-Z\d]? *\d[A-Z]{2}$", postcode.strip(), re.IGNORECASE
            )
            return match.group(0)
        except AttributeError:
            return ""
    return ""


def inherit_property(stream, prop_name):
    """
    Reads a property from StartTable and sets that property (if it exists) on every event between this event
    and the next EndTable event.

    :param event: A filtered list of event objects of type StartTable
    :param prop_name: The property name to inherit
    :return: An updated list of event objects
    """
    prop_value = None
    for event in stream:
        if isinstance(event, events.StartTable):
            prop_value = getattr(event, prop_name, None)
        elif isinstance(event, events.EndTable):
            prop_value = None

        if prop_value and not hasattr(event, prop_name):
            event = event.from_event(event, **{prop_name: prop_value})

        yield event


def check_file_type(input, file_type, la_log_dir):
    """
    Check that the correct type of file is being used, e.g. xml

    :param input: Location of file to be cleaned
    :param file_type: A string of the expected file type extension e.g. "xml"
    :param la_log_dir: Location to save the error log
    :return: Continue if correct, error log if incorrect
    """
    all_allowed_file_types = [
        ".xml",
        ".csv",
        ".xlsx",
        ".xlsm"
    ]
    disallowed_file_types = list(set(all_allowed_file_types).difference([f".{file_type}"]))  # Add "." to start of
    # file_type and wrap in list so .difference works as expected

    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    extension = str(Path(input).suffix)
    filename = str(Path(input).resolve().stem)

    if extension in disallowed_file_types:
        assert extension == f".{file_type}", f"File not in the expected .{file_type} format"

    elif extension == f".{file_type}":
        pass

    else:
        with open(
                f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
                "a",
        ) as f:
            f.write(
                f"File: '{filename}{extension}' not in any of the expected formats (csv, xml, xlsx, xlsm)"
            )
        exit()
