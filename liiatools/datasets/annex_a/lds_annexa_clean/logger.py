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


def create_formatting_error_count(stream):
    """
    Create a list of the column headers for cells with formatting errors (event.error = 1) for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with error counts
    """
    formatting_error_count = None
    for event in stream:
        if isinstance(event, events.StartTable):
            formatting_error_count = []
        elif isinstance(event, events.EndTable):
            yield ErrorTable.from_event(
                event, formatting_error_count=formatting_error_count
            )
            formatting_error_count = None
        elif formatting_error_count is not None and isinstance(event, events.Cell):
            try:
                if event.error == "1":
                    formatting_error_count.append(event.column_header)
            except AttributeError:
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
        blank = event.other_config["canbeblank"]
        if blank is False and event.value is None:
            return event.from_event(event, blank_error="1")
        else:
            return event
    except KeyError:  # Raised in case there is no config item for the given cell
        pass


def create_blank_error_count(stream):
    """
    Create a list of the column headers for cells with blank fields that should not be blank (event.blank_error = 1)
    for each table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    blank_error_count = None
    for event in stream:
        try:
            if isinstance(event, events.StartTable):
                blank_error_count = []
            elif isinstance(event, ErrorTable):
                yield ErrorTable.from_event(event, blank_error_count=blank_error_count)
                blank_error_count = None
            elif blank_error_count is not None and isinstance(event, events.Cell):
                try:
                    if event.blank_error == "1":
                        blank_error_count.append(event.column_header)
                except AttributeError:
                    pass
            yield event
        except Exception as ex:
            log.exception(f"Error occurred in {event.filename}")


def inherit_extra_column_error(stream):
    """
    Add the extra_column_error value to the ErrorTable so these errors can be written to the log.txt file

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    extra_column_error = []
    for event in stream:
        try:
            if isinstance(event, events.StartTable):
                extra_column_error = event.extra_columns
            elif isinstance(event, events.EndTable):
                extra_column_error = []
            elif isinstance(event, ErrorTable):
                yield ErrorTable.from_event(
                    event, extra_column_error=extra_column_error
                )
            yield event
        except AttributeError:
            yield event


def _duplicate_columns(columns_list):
    """
    Return a list of duplicate items within a given list

    :param columns_list: A list of values
    :return: A list of duplicated values
    """
    unique_columns = set()
    duplicate_columns = []
    for column in columns_list:
        if column not in unique_columns:
            unique_columns.add(column)
        else:
            duplicate_columns.append(column)
    return duplicate_columns


@streamfilter(
    check=type_check(events.StartTable),
    fail_function=pass_event,
    error_function=pass_event,
)
def duplicate_column_check(event):
    """
    Create a duplicate_columns_error object for any StartTable that has correctly matched columns but some duplicates

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
    try:
        column_headers = event.matched_column_headers
        if len(set(column_headers)) == len(column_headers):
            return event
        else:
            duplicate_columns = _duplicate_columns(column_headers)
            duplicate_columns = str(duplicate_columns)[1:-1]  # "Remove [ and ] from string
            return event.from_event(event, duplicate_column_error=f"Sheet with title {event.sheet_name} contained "
                                                                  f"the following duplicate column(s): "
                                                                  f"{duplicate_columns}")
    except KeyError:  # Raised in case there is no matched_column_headers
        pass


def inherit_duplicate_column_error(stream):
    """
    Add the duplicate_columns_error value to the ErrorTable so these errors can be written to the log.txt file

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    duplicate_column_error = []
    for event in stream:
        try:
            if isinstance(event, events.StartTable):
                duplicate_column_error = event.duplicate_columns_error
            elif isinstance(event, events.EndTable):
                duplicate_column_error = []
            elif isinstance(event, ErrorTable):
                yield ErrorTable.from_event(
                    event, duplicate_column_error=duplicate_column_error
                )
            yield event
        except AttributeError:
            yield event


def save_errors_la(stream, la_log_dir):
    """
    Count the error events and save them as a text file in the Local Authority Logs directory
    only save the error events if there is at least one error in said event

    :param stream: A filtered list of event objects
    :param la_log_dir: Location to save the gathered error logs
    :return: An updated list of event objects
    """
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    for event in stream:
        try:
            if isinstance(event, ErrorTable) and (
                event.formatting_error_count is not None
                and event.blank_error_count is not None
                and event.sheet_name is not None
                and event.extra_column_error is not None
                and event.duplicate_column_error is not None
            ):
                if (
                    event.formatting_error_count
                    or event.blank_error_count
                    or event.extra_column_error
                    or event.duplicate_column_error
                ):
                    with open(
                        f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                        "a",
                    ) as f:
                        f.write(f"{event.filename}_{start_time}")
                        f.write("\n")
                        f.write("\n")
                        f.write(event.sheet_name)
                        f.write("\n")
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
                        if event.extra_column_error:
                            extra_column_error_no_none = list(
                                filter(None, event.extra_column_error)
                            )
                            f.write(
                                f"Headers of unexpected columns that have been removed or reformatted: "
                                f"{extra_column_error_no_none}"
                            )
                            f.write("\n")
                        if event.duplicate_column_error:
                            f.write(event.duplicate_column_error)
                            f.write("\n")
        except AttributeError:
            pass

        try:
            if isinstance(event, events.StartTable) and event.match_error is not None:
                with open(
                    f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                    "a",
                ) as f:
                    f.write("\n")
                    f.write(event.match_error)
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
    stream = create_formatting_error_count(stream)
    stream = create_blank_error_count(stream)
    stream = inherit_extra_column_error(stream)
    stream = duplicate_column_check(stream)
    stream = inherit_duplicate_column_error(stream)
    return stream
