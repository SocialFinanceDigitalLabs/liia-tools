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


def _check_range(value, min_value=None, max_value=None):
    if not (min_value is None or value >= min_value) and (max_value is None or value <= max_value):
        raise ValueError(f"Value: {value} not in acceptable range: {min_value}-{max_value}")
    return value


@allow_blank
def to_numeric(value, _type, min_value=None, max_value=None, decimal_places=0):
    """
    Convert any strings that should be numeric values based on the config into numeric values.

    :param value: Some value to convert to a number
    :param _type: Type of numeric value, integer or float
    :param min_value: Minimum value allowed
    :param max_value: Maximum value allowed
    :param decimal_places: The number of decimal places to apply
    :return: Either a numeric value or a blank string
    """
    try:
        value = float(value)
        _check_range(value, min_value, max_value)
        if _type == "float":
            return round(value, decimal_places)
        elif _type == "integer":
            return int(value)
    except Exception as e:
        raise ValueError(f"Invalid numeric: {value}") from e


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
