import logging
from datetime import date, datetime

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


def coerce_date(value):
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        try:
            return parser.isoparse(value).date()
        except ValueError:
            pass
        return parser.parse(value, dayfirst=True).date()

    if isinstance(value, numbers.Number):
        return datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(value) - 2).date()
