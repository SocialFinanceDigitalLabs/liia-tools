import logging

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
