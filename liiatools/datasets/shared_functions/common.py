from datetime import datetime
import re
import logging

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
    if postcode != "":
        try:
            match = re.search(
                r"^[A-Z]{1,2}\d[A-Z\d]? *\d[A-Z]{2}$", postcode.strip(), re.IGNORECASE
            )
            return match.group(0)
        except AttributeError as ex:
            return ""
    return ""


def to_short_postcode(postcode):
    """
    Remove whitespace from the beginning and end of postcodes and the last two digits for anonymity
    return blank if not in the right format
    :param postcode: A string with a UK-style post code
    :return: a shortened post code with the area, district, and sector. The units is removed
    """
    try:
        match = re.search(
            r"([A-Z]+\d\d?[A-Z]?)\s*(\d+)([A-Z]{2})", postcode, re.IGNORECASE
        )
        postcode = match.group(1) + " " + match.group(2)
    except (AttributeError, TypeError, ValueError):
        postcode = ""
    return postcode


def to_month_only_dob(dob):
    """
    Convert dates of birth into month and year of birth starting from 1st of each month for anonymity
    return blank if not in the right format
    :param dob: A date of birth datetime object
    :return: A date of birth datetime object with the month rounded to the first day
    """
    try:
        dob = dob.replace(day=1)
    except (AttributeError, TypeError, ValueError):
        dob = ""
    return dob
