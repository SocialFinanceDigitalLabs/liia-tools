import logging
from xmlschema import XMLSchemaValidatorError
import re

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.collectors import collector, block_check

log = logging.getLogger(__name__)


def _get_validation_error(
    event, schema, node, LAchildID_error, field_error
) -> XMLSchemaValidatorError:
    """
    Validate an event

    :param event: A filtered list of event objects
    :param schema: The xml schema attached to a given event
    :param node: The node attached to a given event
    :param LAchildID_error: An empty list to save child ID errors
    :param field_error: An empty list to save field errors
    :return: None if valid, event and error information if XMLSchemaValidatorError, event if AttributeError
    """
    try:
        schema.validate(node)
        return None
    except XMLSchemaValidatorError as e:
        regline = re.compile(
            r"(?=\(line.*?(\w+))", re.MULTILINE
        )  # Search for the number after "line" in error
        missing_field_line = regline.search(str(e)).group(1)
        if "Unexpected" and "LAchildID" in e.reason:
            LAchildID_error.append(
                f"LAchildID is missing from the node starting on line: {missing_field_line}, other errors associated "
                f"to this LAchildID will not be shown because of this"
            )
            return event
        if " expected" in e.reason:
            regexp = re.compile(
                r"(?=\sTag.*?(\w+))"
            )  # Search for the first word after "Tag"
            missing_field = regexp.search(e.reason).group(1)
            field_error.append(
                f"Missing required field: '{missing_field}' which occurs in the node starting "
                f"on line: {missing_field_line}"
            )
            return event
        else:
            if "failed validating ''" in e.message:
                return event.from_event(event, reason="blank")
            else:
                return event
    except AttributeError:  # Return event information, so it can be written to a log for the Local Authority
        return event


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event, LAchildID_error, field_error):
    """
    Validates each element, and if not valid, sets the properties:

    :param event: A filtered list of event objects
    :param LAchildID_error: An empty list to save child ID errors
    :param field_error: An empty list to save field errors

    * valid - (always False)
    * validation_message - a descriptive validation message
    """
    validation_error = _get_validation_error(
        event, event.schema, event.node, LAchildID_error, field_error
    )
    if validation_error is None:
        return event

    if hasattr(validation_error, "reason"):
        message = validation_error.reason
        return events.StartElement.from_event(
            event, valid=False, validation_message=message
        )
    else:
        return events.StartElement.from_event(event, valid=False)


@collector(check=block_check(events.StartElement), receive_stream=True)
def remove_invalid(stream, tag_list):
    """
    Filters out events with the given tag name if they are not valid

    :param stream: A filtered list of event objects
    :param tag_list: A list of node tags
    :return: An updated list of event objects
    """
    stream = list(stream)
    first = stream[0]
    last = stream[-1]
    stream = stream[1:-1]

    if first.tag in tag_list and not getattr(first, "valid", True):
        yield from []
    else:
        yield first

        if len(stream) > 0:
            yield from remove_invalid(stream, tag_list=tag_list)

        yield last
