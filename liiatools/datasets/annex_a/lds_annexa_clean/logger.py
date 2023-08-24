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


def create_error_table(stream):
    """
    Create an ErrorTable event containing the sheet name for that table

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with error counts
    """
    sheet_name = None
    for event in stream:
        if isinstance(event, events.StartTable):
            sheet_name = getattr(event, "sheet_name", None)
        elif isinstance(event, events.EndTable):
            yield ErrorTable.from_event(
                event,
                sheet_name=sheet_name,
            )
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
        allowed_blank = event.other_config["canbeblank"]
        formatting_error = getattr(event, "formatting_error", "0")
        if (
            not allowed_blank
            and not event.value
            and event.value != 0
            and formatting_error != "1"
            and getattr(event, "blank_row", 0) != "1"
        ):
            return event.from_event(event, blank_error="1")
        else:
            return event
    except KeyError:  # Raised in case there is no config item for the given cell
        pass


def create_error_list(stream, error_name):
    """
    Create a list of the column headers for cells with errors

    :param stream: A filtered list of event objects
    :param error_name: A string containing the error_name to inherit e.g. "blank_error"
    :return: An updated list of event objects
    """
    error_list = None
    for event in stream:
        if isinstance(event, events.StartTable):
            error_list = []
        elif isinstance(event, ErrorTable):
            yield ErrorTable.from_event(event, **{f"{error_name}_list": error_list})
            error_list = None
        elif error_list is not None and isinstance(event, events.Cell):
            if getattr(event, error_name, None) == "1":
                error_list.append(event.column_header)
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
            duplicate_columns = str(duplicate_columns)[
                1:-1
            ]  # "Remove [ and ] from string
            return event.from_event(
                event,
                duplicate_columns=f"Sheet with title {event.sheet_name} contained "
                f"the following duplicate column(s): "
                f"{duplicate_columns}",
            )
    except KeyError:  # Raised in case there is no matched_column_headers
        pass
    return event


def inherit_error(stream, error_name):
    """
    Add the error_name_error value to the ErrorTable so these errors can be written to the log.txt file

    :param stream: A filtered list of event objects
    :param error_name: A string containing the error_name to inherit e.g. "duplicate_columns"
    :return: An updated list of event objects
    """
    error = []
    for event in stream:
        try:
            if isinstance(event, events.StartTable):
                error = getattr(event, error_name, [])
            elif isinstance(event, events.EndTable):
                error = []
            elif isinstance(event, ErrorTable):
                yield ErrorTable.from_event(event, **{f"{error_name}_error": error})
            yield event
        except AttributeError:
            yield event


@streamfilter(
    check=type_check(events.StartTable),
    fail_function=pass_event,
    error_function=pass_event,
)
def create_file_match_error(event):
    """
    Add a match_error to StartTables that do not have an event.sheet_name so these errors can be written to the log.txt
    file. If there is no event.sheet_name for a given StartTable that means its headers did not match any of those
    in the config file

    :param event: A filtered list of event objects
    :return: An updated list of event objects
    """
    try:
        if event.sheet_name:
            return event
    except AttributeError:
        return event.from_event(
            event,
            match_error=f"Failed to find a set of matching columns headers for sheet titled "
            f"'{event.name}' which contains column headers "
            f"{event.column_headers}",
        )
    return event


def _missing_sheet_match(sheet_names, expected_sheet_names):
    """
    Return a list of sheet_names missing from the expected_sheet_names

    :param sheet_names: A list of sheet names
    :param expected_sheet_names: A list of expected sheet names
    :return: A list of sheet names missing from the expected sheet names
    """
    return list(set(expected_sheet_names).difference(sheet_names))


def create_missing_sheet_error(stream):
    """
    Checks for any missing sheet names against a list of expected sheet names

    :param stream: A filtered list of event objects
    :return: An updated list of event objects and a list of missing sheets
    """
    sheet_names = []
    expected_sheet_names = [
        "List 1",
        "List 2",
        "List 3",
        "List 4",
        "List 5",
        "List 6",
        "List 7",
        "List 8",
        "List 9",
        "List 10",
        "List 11",
    ]
    for event in stream:
        if isinstance(event, events.StartTable):
            sheet_name = getattr(event, "sheet_name", None)
            sheet_names.append(sheet_name)
        if isinstance(event, events.EndContainer):
            missing_sheet_error = _missing_sheet_match(
                sheet_names, expected_sheet_names
            )
            if missing_sheet_error:
                missing_sheet_error = str(missing_sheet_error)[
                    1:-1
                ]  # Remove brackets [] from list
                yield event.from_event(
                    event,
                    missing_sheet_error=f"The following sheets are missing: "
                    f"{missing_sheet_error} so no output has been "
                    f"created",
                )
            else:
                yield event
        else:
            yield event


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
                and event.sheet_name is not None
                and event.extra_columns_error is not None
                and event.duplicate_columns_error is not None
            ):
                if (
                    event.formatting_error_list
                    or event.blank_error_list
                    or event.extra_columns_error
                    or event.duplicate_columns_error
                ):
                    with open(
                        f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                        "a",
                    ) as f:
                        f.write("\n")
                        f.write(event.sheet_name)
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
                        if event.extra_columns_error:
                            extra_columns_error_no_none = list(
                                filter(None, event.extra_columns_error)
                            )
                            f.write(
                                f"Headers of unexpected columns that have been removed or reformatted: "
                                f"{extra_columns_error_no_none}"
                            )
                            f.write("\n")
                        if event.duplicate_columns_error:
                            f.write(event.duplicate_columns_error)
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

        try:
            if (
                isinstance(event, events.EndContainer)
                and event.missing_sheet_error is not None
            ):
                with open(
                    f"{os.path.join(la_log_dir, event.filename)}_error_log_{start_time}.txt",
                    "a",
                ) as f:
                    f.write("\n")
                    f.write(event.missing_sheet_error)
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
    stream = create_error_table(stream)
    stream = blank_error_check(stream)
    stream = create_error_list(stream, error_name="formatting_error")
    stream = create_error_list(stream, error_name="blank_error")
    stream = inherit_error(stream, error_name="extra_columns")
    stream = duplicate_column_check(stream)
    stream = inherit_error(stream, error_name="duplicate_columns")
    stream = create_file_match_error(stream)
    stream = create_missing_sheet_error(stream)
    return stream
