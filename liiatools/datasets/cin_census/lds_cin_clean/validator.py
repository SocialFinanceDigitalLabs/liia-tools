import logging
from xmlschema import XMLSchemaValidatorError

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


def _get_validation_error(event, schema, node) -> XMLSchemaValidatorError:
    """
    Validate an event

    :param event: A filtered list of event objects
    :param schema: The xml schema attached to a given event
    :param node: The node attached to a given event
    :return: None if valid, error log if XMLSchemaValidatorError, event if AttributeError
    """
    try:
        schema.validate(node)
        return None
    except XMLSchemaValidatorError as e:
        return e
    except AttributeError:  # Return event information, so it can be written to a log for the Local Authority
        return event


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event):
    """
    Validates each element, and if not valid, sets the properties:

    * valid - (always False)
    * validation_message - a descriptive validation message
    """
    validation_error = _get_validation_error(event, event.schema, event.node)
    if validation_error is None:
        return event

    try:
        message = (
            validation_error.reason
            if hasattr(validation_error, "reason")
            else validation_error.message
        )
        return events.StartElement.from_event(
            event, valid=False, validation_message=message
        )
    except AttributeError:
        return events.StartElement.from_event(event, valid=False)
