import logging

from liiatools.datasets.shared_functions.converters import allow_blank

from .spec.__data_schema import Column

log = logging.getLogger(__name__)


@allow_blank
def to_category(value: str, column: Column):
    """
    Matches a string to a category based on categories given in a config file
    the config file should contain a dictionary for each category for this function to loop through
    return blank if no categories found

    :param string: Some string to convert into a category value
    :param categories: A list of dictionaries containing different category:value pairs
    :return: Either a category value, "error" or blank string
    """
    match = column.match_category(str(value).strip())
    if match:
        return match

    raise ValueError(f"Could not match {value} to a category")
