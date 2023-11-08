"""Converter functions for the LIIA tools. These functions are used to ensure that data is in the right format.

All the functions follow a similar format. They take a value, check that it is in the right format, and return the
value as the correct type / with correct format if it is. 
"""
import re
from datetime import date, datetime
from warnings import warn

POSTCODE_PATTERN = re.compile(r"^([A-Z]+\d\d?[A-Z]?)\s*(\d+)([A-Z]{2})$")


def allow_blank(func):
    """A decorator that adds a kwarg to the decorated function to turn on/off the blank value check.

    The allow_blank argument is used to allow a blank value to be returned if the value is blank. Both null and a
    whitespace only string are considered blank.

    The argument is True by default, but this functionality can be turned off by passing allow_blank=False to the
    decorated function. In this case, the value will be passed through to the function to handle accordingly.
    """

    def wrapper(value, *args, allow_blank=True, **kwargs):
        is_blank = value is None or (isinstance(value, str) and value.strip() == "")
        if is_blank:
            if allow_blank:
                return ""
            else:
                raise ValueError("Blank value not allowed")
        return func(value, *args, **kwargs)

    return wrapper


def _match_postcode(value):
    """Cleans and matches a postcode against the UK postcode regex pattern"""
    value = str(value)
    value = re.sub(r"\s+", "", value)
    value = value.upper()

    match = POSTCODE_PATTERN.match(value)
    if not match:
        raise ValueError(f"Invalid postcode: {value}")
    return match


@allow_blank
def to_postcode(value):
    """
    Checks that the postcodes are in the right format
    :param postcode: A string with a UK-style post code
    :return: the correctly formatted postcode
    :raises: ValueError if the postcode is not in the right format
    """
    match = _match_postcode(value)
    return f"{match.group(1)} {match.group(2)}{match.group(3)}"


@allow_blank
def to_short_postcode(value):
    """
    Remove whitespace from the beginning and end of postcodes and the last two digits for anonymity
    return blank if not in the right format
    :param postcode: A string with a UK-style post code
    :return: a shortened post code with the area, district, and sector. The units is removed
    """
    match = _match_postcode(value)
    return f"{match.group(1)} {match.group(2)}"


@allow_blank
def to_integer(value):
    """
    Convert any strings that should be integers based on the config into integers.

    :param value: Some value to convert to an integer
    :return: Either an integer value or a blank string
    """
    try:
        return int(float(value))
    except Exception as e:
        raise ValueError(f"Invalid integer: {value}") from e


@allow_blank
def to_float(value):
    """
    Convert any strings that should be floats based on the config into floats.

    :param value: Some value to convert to a float
    :return: Either a float value or a blank string
    """
    try:
        return float(value)
    except Exception as e:
        raise ValueError(f"Invalid float: {value}") from e


@allow_blank
def to_date(value, dateformat="%d/%m/%Y"):
    """
    Convert a string to a date based on the dateformat %d/%m/%Y and convert a datetime to a date
    :param datevalue: A value to test and make sure it's a datetime object
    :param dateformat: A format for the date to be read correctly, default to %d/%m/%Y
    :return: Either the specified date, converted to a datetime, or an empty string
    """
    try:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        elif isinstance(value, str):
            return datetime.strptime(value, dateformat).date()
    except Exception as e:
        raise ValueError(f"Invalid date: {value}") from e


def to_month_only_dob(*args, **kwargs):
    warn(
        "This function is deprecated. Use to_nth_of_month instead.",
        DeprecationWarning,
    )
    return to_nth_of_month(*args, **kwargs)


@allow_blank
def to_nth_of_month(value: date, n: int = 1):
    """
    Converts dates to the nth day of the month. n defaults to first of the month
    :param dob: A date datetime object
    :return: A date of birth datetime object with the month rounded to the nth day
    """
    try:
        return value.replace(day=n)
    except Exception as e:
        raise ValueError(f"Invalid date: {value}") from e
