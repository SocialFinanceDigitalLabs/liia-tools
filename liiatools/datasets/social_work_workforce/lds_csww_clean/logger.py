from collections import Counter
from datetime import datetime
import logging
import os

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

log = logging.getLogger(__name__)


class ErrorTable(events.ParseEvent):
    pass


def create_formatting_error_list(stream):
    """
    Create a list of the column headers for cells with formatting errors (event.error = 1) for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with error counts
    """
    formatting_error_list = None
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == "LALevelVacancies":
            formatting_error_list = []
        elif isinstance(event, events.EndElement) and event.tag == "Message":
            yield ErrorTable.from_event(
                event,
                formatting_error_list=formatting_error_list,
            )
            formatting_error_list = None
        elif formatting_error_list is not None and isinstance(event, events.TextNode):
            try:
                if event.formatting_error == "1":
                    formatting_error_list.append(event.schema.name)
            except AttributeError:  # Raised in case there is no event.formatting_error
                pass
        yield event


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def blank_error_check(event):
    """
    Check all the values against the config to see if they are allowed to be blank
    if they are blank but should not be, record this as event.blank_error = 1

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    try:
        allowed_blank = event.schema_dict["canbeblank"]
        formatting_error = getattr(event, "formatting_error", "0")
        if not allowed_blank and not event.text and formatting_error != "1":
            return event.from_event(event, blank_error="1")
        else:
            return event
    except AttributeError:  # Raised in case there is no schema dict for the given cell
        pass


def create_blank_error_list(stream):
    """
    Create a list of the column headers for cells with blank fields that should not be blank (event.blank_error = 1)
    for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    blank_error_list = None
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == "LALevelVacancies":
            blank_error_list = []
        elif isinstance(event, events.EndElement) and event.tag == "Message":
            blank_error_list = None
        elif isinstance(event, ErrorTable):
            yield ErrorTable.from_event(event, blank_error_list=blank_error_list)
            blank_error_list = None
        elif blank_error_list is not None and isinstance(event, events.TextNode):
            try:
                if event.blank_error == "1":
                    blank_error_list.append(event.schema.name)
            except AttributeError:  # Raised in case there is no event.blank_error
                pass
        yield event


def create_validation_error_list(stream):
    """
    Create a list of the validation errors

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    validation_error_list = []
    for event in stream:
        if isinstance(event, ErrorTable):
            yield ErrorTable.from_event(
                event, validation_error_list=validation_error_list
            )
            validation_error_list = None
        elif isinstance(event, events.StartElement):
            validation_message = getattr(event, "validation_message", None)
            if validation_message is not None:
                validation_error_list.append(validation_message)
        yield event


def save_errors_la(stream, la_log_dir, filename):
    """
    Count the error events and save them as a text file in the Local Authority Logs directory
    only save the error events if there is at least one error in said event

    :param stream: A filtered list of event objects
    :param la_log_dir: Location to save the gathered error logs
    :param filename: Filename to use
    :return: An updated list of event objects
    """
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"
    for event in stream:
        try:
            if isinstance(event, ErrorTable) and (
                event.formatting_error_list is not None
                and event.blank_error_list is not None
                and event.validation_error_list is not None
            ):
                if (
                    event.formatting_error_list
                    or event.blank_error_list
                    or event.validation_error_list
                ):
                    with open(
                        f"{os.path.join(la_log_dir, filename)}_error_log_{start_time}.txt",
                        "a",
                    ) as f:
                        f.write("\n")
                        if event.formatting_error_list:
                            f.write(
                                "Number of cells that have been made blank "
                                "because they could not be formatted correctly"
                            )
                            f.write("\n")
                            counter_dict = Counter(event.formatting_error_list)
                            f.write(
                                str(counter_dict)[9:-2]
                            )  # Remove "Counter({" and "})" from string
                            f.write("\n")
                        if event.blank_error_list:
                            f.write(
                                "Number of blank cells that should have contained data"
                            )
                            f.write("\n")
                            blank_counter_dict = Counter(event.blank_error_list)
                            f.write(
                                str(blank_counter_dict)[9:-2]
                            )  # Remove "Counter({" and "})" from string
                            f.write("\n")
                        if event.validation_error_list:
                            event.validation_error_list = list(
                                dict.fromkeys(event.validation_error_list)
                            )  # Remove duplicate information from list but
                            # keep order
                            for item in event.validation_error_list:
                                f.write(item)
                                f.write("\n")
        except AttributeError:
            pass

        yield event


def log_errors(stream):
    """
    Compile the log error functions

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    stream = blank_error_check(stream)
    stream = create_formatting_error_list(stream)
    stream = create_blank_error_list(stream)
    stream = create_validation_error_list(stream)
    return stream
