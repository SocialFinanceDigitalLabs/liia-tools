import logging
from xmlschema import XMLSchemaValidatorError

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
        if "Unexpected" and "LAchildID" in e.reason:
            LAchildID_error.append(
                "LAchildID is missing, other errors associated to this LAchildID will not be "
                "shown because of this. See more information below"
            )
            LAchildID_error.append(str(e))
            return event
        if "Unexpected" in e.reason:
            field_error.append(
                f"Essential fields not found. See more information below"
            )
            field_error.append(str(e))
            return event
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
