import logging
from datetime import date, datetime
import parser
import numbers

log = logging.getLogger(__name__)


def to_category(string, categories):
    """
    Matches a string to a category based on categories given in a config file
    the config file should contain a dictionary for each category for this function to loop through
    return blank if no categories found

    :param string: Some string to convert into a category value
    :param categories: A list of dictionaries containing different category:value pairs
    :return: Either a category value, "error" or blank string
    """
    for code in categories:
        if str(string).lower() == str(code["code"]).lower():
            return code["code"]
        if (
            str(string).lower() == str(code["code"]).lower() + ".0"
        ):  # In case integers are read as floats
            return code["code"]
        elif "name" in code:
            if str(code["name"]).lower() in str(string).lower():
                return code["code"]
            elif not string:
                return ""
        elif not string:
            return ""
    return "error"


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
        return datetime.fromordinal(
            datetime(1900, 1, 1).toordinal() + int(value) - 2
        ).date()
