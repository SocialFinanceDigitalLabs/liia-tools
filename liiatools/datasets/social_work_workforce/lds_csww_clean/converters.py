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


def to_integer(value, config):
    """
    Convert any strings that should be integers based on the config into integers

    :param value: Some value to convert to an integer
    :param config: The loaded configuration
    :return: Either an integer value or an "error" string if value could not be formatted as integer or a blank string if no value provided
    """
    if config == "integer":
        if value or value==0:
            if isinstance(value, str) and value[-2:] == ".0":
                return int(float(value))
            elif value or value == 0:
                return int(value)
            else:
                return "error" # value incorrectly formatted
        return "" # no value provided
    else:
        return value


def to_decimal(value, config, decimal_places=0):
    """
    Convert any strings that should be decimal based on the config into decimals

    :param value: Some value to convert to a decimal
    :param config: The loaded configuration
    :param dec_places: The number of decimal places to apply (default 0) 
    :return: Either a decimal value formatted to number of decimal places or an "error" string if value could not be formatted as decimal or a blank string if no value provided
    """
    if config == "decimal":
        if value or value == 0:
            try:
                float(value)
                round_to_dp = round(float(value), decimal_places)
                return round_to_dp
            except (ValueError, TypeError):
                return "error" # value incorrectly formatted
        return "" # no value provided
    return value


def to_regex(value, pattern):
    """
    Convert any strings that should conform to regex pattern based on the schema into regex string

    :param value: Some value to convert to a regex string
    :param pattern: The regex pattern to compare
    :return: Either a string matching the regex pattern or an "error" string if value does not match pattern or a blank string if no value provided
    """
    if pattern:
        if value:
            stripped_value = value.strip()
            try:
                isfullmatch = re.fullmatch(pattern, stripped_value)
                if isfullmatch:
                    return stripped_value
                else:
                    return "error" # value does not match regex pattern
            except (ValueError, TypeError):
                return "error" # value incorrectly formatted
        else:
            return "" # no value provided
    else:
        return value