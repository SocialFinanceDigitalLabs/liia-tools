from datetime import datetime
import re
import logging

log = logging.getLogger(__name__)


def to_category(string, categories):
    """
    Matches a string to a category based on categories given in a config file
    the config file should contain a dictionary for each category for this function to loop through
    return blank if no categories found
    """
    for code in categories:
        if str(string).lower() == str(code["code"]).lower():
            return code["code"]
        elif "name" in code:
            if str(code["name"]).lower() in str(string).lower():
                return code["code"]
            else:
                return "error"
        elif string is None:
            return ""
        elif string == "":
            return ""
        else:
            return "error"


def to_integer(string, config):
    """
    Convert any strings that should be integers based on the config into integers
    """
    if config == "integer" and string != "" and string is not None:
        string = int(string)
        return string
    elif config == "integer" and string == "":
        return string
    elif config == "integer" and string is None:
        return ""
    else:
        return string


def to_date(string, dateformat):
    """
    Convert a string to a date based on the config (%d/%m/%Y for example) and convert a datetime to a date
    """
    if isinstance(string, datetime):
        string = string.date()
    elif isinstance(string, str) and string != "":
        string = datetime.strptime(string, dateformat).date()  # Check this is possible
    elif string is None:
        string = ""
    elif string == "":
        string = ""
    return string
