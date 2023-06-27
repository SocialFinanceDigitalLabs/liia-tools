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
    Create a list of the column headers for cells with formatting errors (event.formatting_error = 1)

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with formatting error lists
    """
    formatting_error_list = None
    for event in stream:
        if isinstance(event, events.StartTable):
            formatting_error_list = []
        elif isinstance(event, events.EndTable):
            yield ErrorTable.from_event(
                event,
                formatting_error_list=formatting_error_list,
            )
            formatting_error_list = None
        elif (
            formatting_error_list is not None
            and isinstance(event, events.Cell)
        ):
            formatting_error = getattr(event, "formatting_error", "0")
            if formatting_error == "1":
                formatting_error_list.append(event.header)
            else:
                pass
        yield event


@streamfilter(
    check=type_check(events.Cell), fail_function=pass_event, error_function=pass_event
)
def blank_error_check(event):
    """
    Check all the values against the config to see if they are allowed to be blank
    if they are blank but should not be, record this as event.blank_error = 1

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    try:
        allowed_blank = event.config_dict["canbeblank"]
        formatting_error = getattr(event, "formatting_error", "0")
        if not allowed_blank and not event.cell and formatting_error != "1":
            return event.from_event(event, blank_error="1")
        else:
            return event
    except AttributeError:  # Raised in case there is no config item for the given cell
        pass


def create_blank_error_list(stream):
    """
    Create a list of the column headers for cells with blank fields that should not be blank (event.blank_error = 1)
    for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with blank error lists
    """
    blank_error_list = None
    for event in stream:
        if isinstance(event, events.StartTable):
            blank_error_list = []
        elif isinstance(event, events.EndTable):
            blank_error_list = None
        elif isinstance(event, ErrorTable):
            yield ErrorTable.from_event(event, blank_error_list=blank_error_list)
            blank_error_list = None
        elif blank_error_list is not None and isinstance(event, events.Cell):
            blank_error = getattr(event, "blank_error", "0")
            if blank_error == "1":
                blank_error_list.append(event.header)
            else:
                pass
        yield event


@streamfilter(
    check=type_check(events.StartTable),
    fail_function=pass_event,
    error_function=pass_event,
)
def create_file_match_error(event):
    """
    Add a match_error to StartTables that do not have an event.expected_columns so these errors can be written to the
    log.txt file. If there is no event.expected_columns for a given StartTable that means its headers did not match
    those in the config file

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
    expected_columns = getattr(event, "expected_columns", None)
    if expected_columns is None:
        return event.from_event(
            event,
            match_error=f"Failed to find a set of matching columns headers for file titled '{event.filename}' "
                        f"which contains column headers {event.headers} so no output has been produced",
        )
    else:
        return event


@streamfilter(
    check=type_check(events.StartTable),
    fail_function=pass_event,
    error_function=pass_event,
)
def create_extra_column_error(event):
    """
    Add an extra_column_error to StartTables that have more columns than the set of expected columns so these can be
    written to the log.txt file.

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
    extra_columns = [
        item for item in event.headers if item not in event.expected_columns
    ]
    if len(extra_columns) == 0:
        return event
    else:
        return event.from_event(
            event,
            extra_column_error=f"Additional columns were found in file titled "
            f"'{event.filename}' than those expected from schema so these columns have been removed: {extra_columns}",
        )


def save_errors_la(stream, la_log_dir):
    """
    Count the error events and save them as a text file in the Local Authority Logs directory
    only save the error events if there is at least one error in said event

    :param stream: A filtered list of event objects
    :param la_log_dir: Location to save the gathered error logs
    :return: An updated list of event objects
    """
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"
    for event in stream:
        try:
            if isinstance(event, ErrorTable) and (
                event.formatting_error_list is not None
                and event.blank_error_list is not None
            ):
                if event.formatting_error_list or event.blank_error_list:
                    with open(
                        f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                        "a",
                    ) as f:
                        if event.formatting_error_count:
                            f.write(
                                "Number of cells that have been made blank "
                                "because they could not be formatted correctly"
                            )
                            f.write("\n")
                            counter_dict = Counter(event.formatting_error_count)
                            f.write(
                                str(counter_dict)[9:-2]
                            )  # Remove "Counter({" and "})" from string
                            f.write("\n")
                        if event.blank_error_count:
                            f.write(
                                "Number of blank cells that should have contained data"
                            )
                            f.write("\n")
                            blank_counter_dict = Counter(event.blank_error_count)
                            f.write(
                                str(blank_counter_dict)[9:-2]
                            )  # Remove "Counter({" and "})" from string
                            f.write("\n")
        except AttributeError:
            pass

        if isinstance(event, events.StartTable):
            match_error = getattr(event, "match_error", None)
            if match_error:
                with open(
                    f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                    "a",
                ) as f:
                    f.write(match_error)
                    f.write("\n")
            column_error = getattr(event, "extra_column_error", None)
            if column_error:
                with open(
                    f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                    "a",
                ) as f:
                    f.write(column_error)
                    f.write("\n")
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
    stream = create_file_match_error(stream)
    stream = create_extra_column_error(stream)
    return stream
