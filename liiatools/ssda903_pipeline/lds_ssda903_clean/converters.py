import logging

from ..spec import Column

log = logging.getLogger(__name__)


def to_category(value: str, column: Column):
    """
    Matches a string to a category based on categories given in a config file
    the config file should contain a dictionary for each category for this function to loop through
    return blank if no categories found

    :param string: Some string to convert into a category value
    :param categories: A list of dictionaries containing different category:value pairs
    :return: Either a category value, "error" or blank string
    """
    value = str(value).strip()
    if value == "":
        return ""
    return column.match_category(value)


def to_integer(value):
    """
    Convert any strings that should be integers based on the config into integers

    :param value: Some value to convert to an integer
    :param config: The loaded configuration
    :return: Either an integer value or a blank string
    """
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
