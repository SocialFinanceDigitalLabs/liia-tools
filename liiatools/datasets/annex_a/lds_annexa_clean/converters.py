from datetime import datetime
import logging

log = logging.getLogger(__name__)


def to_integer(value):
    """
    Convert any strings that should be integers based on the config into integers
    :param value: Some value to convert to an integer
    :return: Either an integer value or a blank string
    """
    if value:
        return int(value)
    return ""


def to_date(datevalue):
    """
    Convert a string to a date based on the dateformat %d/%m/%Y and convert a datetime to a date
    :param datevalue: A value to test and make sure it's a datetime object
    :return: Either the specified date, converted to a datetime, or an empty string
    """
    if isinstance(datevalue, datetime):
        return datevalue.date()
    elif isinstance(datevalue, str) and datevalue:
        return datetime.strptime(datevalue, "%d/%m/%Y").date()  # Check this is possible
    return ""
