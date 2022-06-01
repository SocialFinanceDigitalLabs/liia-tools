from datetime import datetime
import re
import logging

log = logging.getLogger(__name__)


def to_integer(string):
    """
    Convert any strings that should be integers based on the config into integers
    """
    if string != "":
        return int(string)
    return ""


def to_date(string):
    """
    Convert a string to a date based on the dateformat %d/%m/%Y and convert a datetime to a date
    """
    if isinstance(string, datetime):
        return string.date()
    elif isinstance(string, str) and string != "":
        return datetime.strptime(string, "%d/%m/%Y").date()  # Check this is possible
    return ""
