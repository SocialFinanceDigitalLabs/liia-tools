from datetime import datetime, date
import re


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
        if isinstance(string, float):
            string = int(string)
        if str(string).lower() == str(code["code"]).lower():
            return code["code"]
        elif "name" in code and string:
            if str(code["name"]).lower() in str(string).lower():
                return code["code"]
        elif not string:
            return ""
    return "formatting_error"


def to_date(datevalue, dateformat="%d/%m/%Y"):
    """
    Convert a string to a date based on the dateformat %d/%m/%Y and convert a datetime to a date
    :param datevalue: A value to test and make sure it's a datetime object
    :param dateformat: A format for the date to be read correctly, default to %d/%m/%Y
    :return: Either the specified date, converted to a datetime, or an empty string
    """
    if datevalue:
        if isinstance(datevalue, datetime):
            return datevalue.date()
        if isinstance(datevalue, date):
            return datevalue
        elif isinstance(datevalue, str):
            return datetime.strptime(datevalue, dateformat).date()
    else:
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
