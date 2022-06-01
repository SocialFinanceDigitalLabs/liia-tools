from datetime import datetime
import re
import logging

log = logging.getLogger(__name__)


def check_postcode(string):
    """
    Checks that the postcodes are in the right format
    """
    if string != "":
        match = re.search(r"[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}", string, re.IGNORECASE)
        return match.group(0)
    return ""


def to_short_postcode(postcode):
    """
    Remove whitespace from the beginning and end of postcodes and the last two digits for anonymity
    return blank if not in the right format
    """
    try:
        postcode = postcode.strip()
        postcode = postcode[:-2]
    except (AttributeError, TypeError, ValueError):
        postcode = ""
    return postcode


def to_month_only_dob(string):
    """
    Convert dates of birth into month and year of birth starting from 1st of each month for anonymity
    return blank if not in the right format
    """
    try:
        string = string.replace(day=1)
    except (AttributeError, TypeError, ValueError):
        string = ""
    return string
