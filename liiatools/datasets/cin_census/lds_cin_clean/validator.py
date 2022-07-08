import logging
from xmlschema import XMLSchemaValidatorError
from pathlib import Path
from datetime import datetime

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


def _save_error(input, la_log_dir, error):
    """
    Save errors to a text file in the LA log directory

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :param error: The error information
    :return: Text file containing the error information
    """
    filename = str(Path(input).resolve().stem)
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
    ) as f:
        f.write(
            f"Could not process {filename} because it is missing a required element(s) as described below"
        )
        f.write("\n")
        f.write(str(error))
        f.write("\n")


def _get_validation_error(event, schema, node, input, la_log_dir) -> XMLSchemaValidatorError:
    """
    Validate an event

    :param event: A filtered list of event objects
    :param schema: The xml schema attached to a given event
    :param node: The node attached to a given event
    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :return: None if valid, error log if XMLSchemaValidatorError, event if AttributeError
    """
    try:
        schema.validate(node)
        return None
    except XMLSchemaValidatorError as e:
        _save_error(input, la_log_dir, error=e)
        raise e
    except AttributeError:  # Return event information, so it can be written to a log for the Local Authority
        return event


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event, input, la_log_dir):
    """
    Validates each element, and if not valid, sets the properties:

    :param event: A filtered list of event objects
    :param input: The input file location, including file name and suffix, and be usable by a Path function

    * valid - (always False)
    * validation_message - a descriptive validation message
    """
    validation_error = _get_validation_error(event, event.schema, event.node, input, la_log_dir)
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
