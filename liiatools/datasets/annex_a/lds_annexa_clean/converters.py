import logging

log = logging.getLogger(__name__)


def to_integer(value):
    """
    Convert any strings that should be integers based on the config into integers
    :param value: Some value to convert to an integer
    :return: Either an integer value or a blank string
    """
    if isinstance(value, str) and value[-2:] == ".0":
        return int(float(value))
    if value or value == 0:
        return int(value)
    else:
        return ""
