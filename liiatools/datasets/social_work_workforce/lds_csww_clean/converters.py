import logging
import re

log = logging.getLogger(__name__)


def to_category(string, categories):
    """
    Matches a string to a category based on categories given in a schema file
    the schema file should contain a dictionary for each category for this function to loop through
    return blank if no categories found

    :param string: Some string to convert into a category value
    :param categories: A list of dictionaries containing different category:value pairs
    :return: Either a category value, "error" if category is invalid or blank string
    """
    for code in categories:
        if str(string).lower() == str(code["code"]).lower():
            return code["code"]
        if (
            str(string).lower() == str(code["code"]).lower() + ".0"
        ):  # In case integers are read as floats
            return code["code"]
        if "name" in code:
            if str(code["name"]).lower() in str(string).lower():
                return code["code"]
            if not string:
                return ""
        elif not string:
            return ""
    return "error"


def to_numeric(value, config, decimal_places=0, min_inclusive=None, max_inclusive=None):
    """
    Convert any strings that should be integer or decimal based on the config into integer or decimal

    :param value: Some value to convert to an integer or decimal
    :param config: The loaded configuration
    :param decimal_places: The number of decimal places to apply (default 0)
    :param min_inclusive: Minimum value allowed (default none)
    :param max_inclusive: Maximum value allowed (default none)
    :return: Either an integer, a decimal value formatted to number of decimal places or an "error" string if
    value could not be formatted as decimal or a blank string if no value provided
    """
    if config == "decimal":
        if value or value == 0:
            round_to_dp = round(float(value), int(decimal_places))
            if (min_inclusive is None or round_to_dp >= float(min_inclusive)) and (
                max_inclusive is None or round_to_dp <= float(max_inclusive)
            ):
                return round_to_dp
            return "error"  # min/max error
        return ""  # no value provided
    if config == "integer":
        if value or value == 0:
            return int(float(value))
        return ""  # no value provided
    else:
        return value


def to_regex(value, pattern):
    """
    Convert any strings that should conform to regex pattern based on the schema into regex string

    :param value: Some value to convert to a regex string
    :param pattern: The regex pattern to compare
    :return: Either a string matching the regex pattern or an "error" string if value does not match pattern or a
    blank string if no value provided
    """
    if value:
        stripped_value = value.strip()
        isfullmatch = re.fullmatch(pattern, stripped_value)
        if isfullmatch:
            return stripped_value
        return "error"  # value does not match regex pattern
    else:
        return ""  # no value provided
