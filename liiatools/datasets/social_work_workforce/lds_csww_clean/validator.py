import logging
import re

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


def _get_validation_error(event, schema, node):
    """
    Validate an event

    :param event: A filtered list of event objects
    :param schema: The xml schema attached to a given event
    :param node: The node attached to a given event
    :return: Event and error information
    """
    try:
        validation_error_iterator = schema.iter_errors(node)
        for validation_error in validation_error_iterator:
            if " expected" in validation_error.reason:

                reg_line = re.compile(
                    r"(?=\(line.*?(\w+))", re.MULTILINE
                )  # Search for the number after "line" in error
                missing_field_line = reg_line.search(str(validation_error)).group(1)

                reg_exp = re.compile(
                    r"(?=\sTag.*?(\w+))"
                )  # Search for the first word after "Tag"
                missing_field = reg_exp.search(validation_error.reason).group(1)

                errors = (
                    f"Missing required field: '{missing_field}' which occurs in the node starting on "
                    f"line: {missing_field_line}"
                )

                return event.from_event(event, reason=errors)

    except AttributeError:  # Raised for nodes that don't exist in the schema
        reason = f"Unexpected node '{event.tag}'"
        return event.from_event(event, reason=reason)


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event):
    """
    Validates each element, and if not valid, sets the properties:

    :param event: A filtered list of event objects

    * valid - (always False)
    * validation_message - a descriptive validation message
    """
    validation_error = _get_validation_error(event, event.schema, event.node)

    if validation_error is None:
        return event

    message = validation_error.reason
    return event.from_event(event, valid=False, validation_message=message)
