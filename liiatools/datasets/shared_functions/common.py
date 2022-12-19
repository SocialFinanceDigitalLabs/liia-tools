import re
import logging
from pathlib import Path
from datetime import datetime

from sfdata_stream_parser import events

log = logging.getLogger(__name__)

supported_file_types = [".xml", ".csv", ".xlsx", ".xlsm"]


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


def save_year_error(input, la_log_dir):
    """
    Save errors to a text file in the LA log directory

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :return: Text file containing the error information
    """
    filename = Path(input).resolve().stem
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    with open(
        f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
        "a",
    ) as f:
        f.write(
            f"Could not process '{filename}' because no year was found in the name of the file"
        )


def check_year(filename):
    """
    Check a filename to see if it contains a year, if it does, return that year
    Expected year formats within string:
        2022
        14032021
        2017-18
        201819
        2019/20
        1920
        21/22
        21-22

    :param filename: Filename that probably contains a year
    :return: Year within the string
    """
    match = re.search(r"(20)\d{2}(.*\d{2})*", filename)
    if match:
        try:
            if len(match.group(2)) == 2:
                year = match.group(1) + match.group(2)
                return year
            if len(match.group(2)) == 3:
                year = match.group(1) + match.group(2)[1:]
                return year
            if len(match.group(2)) == 4:
                year = match.group(2)
                return year
        except TypeError:
            year = match.group(0)
            return year

    fy_match = re.search(r"\d{2}(.*\d{2})", filename)
    if fy_match:
        if len(fy_match.group(1)) == 2 and int(fy_match.group(1)) == int(fy_match.group(0)[:2]) + 1:
            year = "20" + fy_match.group(1)
            return year
        if len(fy_match.group(1)) == 3 and int(fy_match.group(1)[-2:]) == int(fy_match.group(0)[:2]) + 1:
            year = "20" + fy_match.group(1)[-2:]
            return year
        else:
            raise AttributeError

    else:
        raise AttributeError


def check_file_type(input, file_types, supported_file_types, la_log_dir):
    """
    Check that the correct type of file is being used, e.g. xml. If it is then continue.
    If not then check if it is in the list of supported file types. If it is then log this error to the data processor
    If it does not match any of the expected file types then log this error to the data controller

    :param input: Location of file to be cleaned
    :param file_types: A list of the expected file type extensions e.g. [".xml", ".csv"]
    :param supported_file_types: A list of file types supported by the process e.g. [".csv", ".xlsx"]
    :param la_log_dir: Location to save the error log
    :return: Continue if correct, error log if incorrect
    """
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    extension = Path(input).suffix
    filename = Path(input).resolve().stem

    disallowed_file_types = list(set(supported_file_types).difference(file_types))

    if extension in file_types:
        pass

    elif extension in disallowed_file_types:
        assert extension in file_types, f"File not in the expected {file_types} format"

    else:
        with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
        ) as f:
            f.write(
                f"File: '{filename}{extension}' not in any of the expected formats (csv, xml, xlsx, xlsm)"
            )
        return "incorrect file type"
