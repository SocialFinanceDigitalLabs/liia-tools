import re
import logging

log = logging.getLogger(__name__)


def flip_dict(some_dict):
    """
    Potentially a temporary function which switches keys and values in a dictionary.
    May need to rewrite LA-codes YAML file to avoid this step

    :param some_dict: A config dictionary
    :return: a reversed dictionary with keys as values and vice versa
    """
    return {value: key for key, value in some_dict.items()}


def check_postcode(postcode):
    """
    Checks that the postcodes are in the right format
    :param postcode: A string with a UK-style post code
    :return: a post code, or if incorrect a blank string
    """
    if postcode:
        match = re.search(
            r"^[A-Z]{1,2}\d[A-Z\d]? *\d[A-Z]{2}$", postcode.strip(), re.IGNORECASE
        )
        return match.group(0)
    else:
        return ""
