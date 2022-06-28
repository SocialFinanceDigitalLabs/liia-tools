import logging
from xmlschema import XMLSchemaValidatorError

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

from liiatools.datasets.cin_census.lds_cin_clean.converters import to_date, to_category

log = logging.getLogger(__name__)


def _get_validation_error(schema, node) -> XMLSchemaValidatorError:
    try:
        schema.validate(node)
        return None
    except XMLSchemaValidatorError as e:
        return e


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event):
    """
    Validates each element, and if not valid, sets the properties:

    * valid - (always False)
    * validation_message - a descriptive validation message
    """
    validation_error = _get_validation_error(event.schema, event.node)
    if validation_error is None:
        return event

    message = validation_error.reason if hasattr(validation_error, 'reason') else validation_error.message
    return events.StartElement.from_event(event, valid=False, validation_message=message)


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    category = event.config_dict["category"]
    try:
        text = to_category(event.cell, category)
        if text != "error":
            return event.from_event(event, cell=text, error="0")
        else:
            return event.from_event(event, cell="", error="1")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    date = event.config_dict["date"]
    try:
        text = to_date(event.cell, date)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")


def clean(stream):
    """
    Compile cleaning functions
    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    stream = clean_categories(stream)
    stream = clean_dates(stream)
    return stream
