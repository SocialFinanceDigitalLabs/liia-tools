import logging

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
