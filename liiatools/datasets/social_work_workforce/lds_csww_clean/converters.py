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
    :return: Either a category value, "error" or blank string
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
    :return: Either an integer value or a blank string
    """
    if config == "integer":
        if isinstance(value, str) and value[-2:] == ".0":
            return int(float(value))
        elif value or value == 0:
            return int(value)
        else:
            return ""
    else:
        return value


def to_decimal(value, config, decplaces=0):
    """
    Convert any strings that should be decimal based on the config into decimals

    :param value: Some value to convert to a decimal
    :param config: The loaded configuration
    :param decplaces: The number of decimal places 
    :return: Either a decimal value formatted to number of decimal places or a blank string
    """
    dpdisplayformat= f".{decplaces}f"
    if config == "decimal":
        try:
            float(value)
            roundtodp = round(float(value), decplaces)
            return f"{roundtodp: {dpdisplayformat}}".strip()
        except (ValueError, TypeError):
            return ""
    return value


def to_regex(value, pattern):
    """
    Convert any strings that should conform to regex pattern based on the schema into regex string

    :param value: Some value to convert to a regex string
    :param pattern: The regex pattern to compare
    :return: Either a regex string or a blank string
    """
    if pattern:
        if value:
            try:
                isfullmatch = re.fullmatch(pattern, value)
                if isfullmatch:
                    return value
                else:
                    return ""
            except (ValueError, TypeError):
                return ""
        else:
            return ""
    else:
        return value