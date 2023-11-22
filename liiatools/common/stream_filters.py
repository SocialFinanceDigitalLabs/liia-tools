import logging
from io import BytesIO, StringIO
from typing import Iterable, Union, Any, Dict, List

import tablib
from sfdata_stream_parser import events, collectors
from tablib import import_book, import_set
from tablib.core import detect_format

from liiatools.common.data import FileLocator
from liiatools.common.stream_errors import StreamError


from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import (
    generator_with_value,
    pass_event,
    streamfilter,
)

from liiatools.common.data import DataContainer
from liiatools.common.stream_errors import EventErrors
from liiatools.datasets.shared_functions.converters import (
    to_date,
    to_numeric,
    to_postcode,
    to_regex
)

from .converters import to_category
from .spec.__data_schema import Column, DataSchema

logger = logging.getLogger(__name__)


def tablib_parse(source: FileLocator):
    """
    Parse any of the tabular formats supported by TabLib
    """
    filename = source.name
    with source.open("rb") as f:
        data = f.read()

    try:
        data = data.decode("utf-8")
        data = StringIO(data)
    except UnicodeDecodeError:
        data = BytesIO(data)

    try:
        databook = import_book(data)
        logger.debug(
            "Opened %s as a book with the following sheets: %s",
            filename,
            [s.title for s in databook.sheets()],
        )
        return tablib_to_stream(databook, filename=filename)
    except Exception as e:
        logger.debug("Failed to open %s as a book", filename, exc_info=e)
        pass

    try:
        dataset = import_set(data)
        logger.debug("Opened %s as a sheet", filename)
        return tablib_to_stream(dataset, filename=filename)
    except Exception as e:
        logger.debug("Failed to open %s as a sheet", filename)
        pass

    raise StreamError(f"Could not parse {source} as a tabular format")


def _tablib_dataset_to_stream(dataset: tablib.Dataset, **kwargs):
    params = {k: v for k, v in kwargs.items() if v is not None}
    yield events.StartContainer(**params)
    yield events.StartTable(headers=dataset.headers)
    for r_ix, row in enumerate(dataset):
        yield events.StartRow()
        for c_ix, cell in enumerate(row):
            yield events.Cell(
                r_ix=r_ix,
                c_ix=c_ix,
                header=dataset.headers[c_ix],
                cell=cell,
            )
        yield events.EndRow()
    yield events.EndTable()
    yield events.EndContainer()


def tablib_to_stream(
    data: Union[tablib.Dataset, tablib.Databook], filename: str = None
):
    """
    Parse the csv and return the row number, column number, header name and cell value

    :param input: Location of file to be cleaned
    :return: List of event objects containing filename, header and cell information
    """
    if isinstance(data, tablib.Dataset):
        logger.debug("Export %s as a single sheet", type(data).__name__)
        yield from _tablib_dataset_to_stream(data, filename=filename)

    elif isinstance(data, tablib.Databook):
        for sheet in data.sheets():
            yield from _tablib_dataset_to_stream(
                sheet, filename=filename, sheetname=sheet.title
            )


def inherit_property(stream, prop_name: Union[str, Iterable[str]], override=False):
    """
    Reads a property from StartTable and sets that property (if it exists) on every event between this event
    and the next EndTable event.

    :param event: A filtered list of event objects of type StartTable
    :param prop_name: The property name to inherit
    :return: An updated list of event objects
    """
    if isinstance(prop_name, str):
        prop_name = [prop_name]

    prop_value = None
    for event in stream:
        if isinstance(event, events.StartTable):
            prop_value = {k: getattr(event, k, None) for k in prop_name}
            prop_value = {k: v for k, v in prop_value.items() if v is not None}
        elif isinstance(event, events.EndTable):
            prop_value = None

        if prop_value:
            if override:
                event_values = prop_value
            else:
                event_values = {
                    k: v for k, v in prop_value.items() if not hasattr(event, k)
                }
            event = event.from_event(event, **event_values)

        yield event


@streamfilter(check=type_check(events.StartTable), fail_function=pass_event)
def add_table_name(event, schema: DataSchema):
    """
    Match the loaded table name against one of the 10 903 file names

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
    headers = getattr(event, "headers", None)
    if not headers:
        table_name = None
    else:
        table_name = schema.get_table_from_headers(event.headers)

    if table_name:
        return event.from_event(
            event, table_name=table_name, table_spec=schema.column_map[table_name]
        )
    else:
        return event


@streamfilter(check=type_check(events.Cell), fail_function=pass_event)
def match_config_to_cell(event, schema: DataSchema):
    """
    Match the cell to the config file given the table name and cell header
    the config file should be a set of dictionaries for each table, headers within those tables
    and config rules for those headers

    Requires:

        * `table_name` on Cell events
        * `header` on Cell events

    Provides:

        * `column_spec` on Cell events

    :param event: A filtered list of event objects of type Cell
    :param config: The loaded configuration to use
    :return: An updated list of event objects
    """
    if hasattr(event, "table_name") and hasattr(event, "header"):
        table_config = schema.table.get(event.table_name)
        if table_config and event.header in table_config:
            return event.from_event(event, column_spec=table_config[event.header])
    return event


@streamfilter(check=type_check((events.Cell, events.TextNode)), fail_function=pass_event)
def log_blanks(event):
    """Creates a EventErrors for cells flagged as not allowing blank but that are empty (None or blank string)."""
    column_spec = getattr(event, "column_spec", None)

    # No specification - we can't check
    if not column_spec:
        return event

    # Not required - we don't need to check
    if column_spec.canbeblank:
        return event

    cell_value = getattr(event, "cell", None) or getattr(event, "text", None)
    if isinstance(cell_value, str):
        cell_value = cell_value.strip()

    if cell_value is None:
        cell_value = ""

    if cell_value == "":
        return EventErrors.add_to_event(
            event, type="Blank", message=f"Blank value for mandatory cell"
        )

    return event


@streamfilter(check=type_check((events.Cell, events.TextNode)), fail_function=pass_event)
def conform_cell_types(event, preserve_value=False):
    """
    A streamfilter that conforms known cell types to the correct type.

    If there is no column_spec on the event, then the event is passed through unchanged.

    If there is a column_spec, then the cell value is converted to the correct type.

    An error is raised if the type is unknown (UnknownType) or if the conversion fails (ConversionError).

    Requires:
        * `column_spec` with the schema specification
        * `cell` with the value to convert

    Provides:
        * `cell` with the converted value
    """
    column_spec = getattr(event, "column_spec", None)
    if not column_spec:
        return event

    if column_spec.type == "category":
        converter = lambda x: to_category(x, column_spec)
    elif column_spec.type == "date":
        converter = lambda x: to_date(x, column_spec.date)
    elif column_spec.type == "numeric":
        converter = lambda x: to_numeric(
            x,
            column_spec.numeric.type,
            column_spec.numeric.min_value,
            column_spec.numeric.max_value,
            column_spec.numeric.decimal_places,
        )
    elif column_spec.type == "postcode":
        converter = to_postcode
    elif column_spec.type == "string":
        converter = lambda x: str(x)
    elif column_spec.type == "regex":
        converter = lambda x: to_regex(x, column_spec.cell_regex)
    else:
        return EventErrors.add_to_event(
            event, type="UnknownType", message=f"Unknown cell type {column_spec.type}"
        )

    cell_value = getattr(event, "cell", None) or getattr(event, "text", None)

    try:
        cell_value = converter(cell_value)
        return event.from_event(event, cell=cell_value)
    except ValueError as e:
        event = event.from_event(event, cell=cell_value if preserve_value else "")
        return EventErrors.add_to_event(
            event,
            type="ConversionError",
            message=f"Could not convert to {column_spec.type}",
            exception=str(e),
        )


@collectors.collector(
    check=collectors.block_check(events.StartRow), receive_stream=True
)
def collect_cell_values_for_row(row):
    """
    Collects the cell values for each row and set these as `column_spec` on the StartRow event.

    Requires:
        * `table_spec` on Cell events to idenfity this column as part of the table
        * `header` on Cell events with the column key
        * `cell` on Cell events with the value

    Provides:
        * `row_values` on StartRow events with a dictionary of column values for the row

    Yields:
        * All the events

    """
    # Read the row
    row = list(row)

    # Get the start and end row events
    start_row = row.pop(0)
    end_row = row.pop(-1)

    # Where we have identified the column, set values on the start row
    values = {}
    for cell in row:
        schema: Column = getattr(cell, "column_spec", None)
        if schema:
            values[cell.header] = cell.cell

    # Yield events
    yield start_row.from_event(start_row, row_values=values)
    yield from row
    yield end_row


@generator_with_value
def collect_tables(stream):
    """Collects all the tables into a dictionary of lists of rows.

    This filter requires that the stream has been processed by `collect_cell_values_for_row` first, or at least
    something that sets `table_name` and `row_values` on StartRow events prior to this filter.

    Requires:
        * `table_name` on StartRow events
        * `row_values` on StartRow events


    Yields:
        * All the events

    Returns:
        * `datasets` - A dictionary of lists of rows, keyed by table name.

    """
    # A dict to hold all the tables we find in the stream
    dataset = {}

    # Iterate over the stream and collect the tables from the StartRow events
    for event in stream:
        if isinstance(event, events.StartRow) and hasattr(event, "table_name"):
            table_data = dataset.setdefault(event.table_name, [])
            table_data.append(event.row_values)

        yield event

    # With the stream fully consumed, we can return the populated dataset
    return dataset


def _populate_error_entry(from_obj: Any, to_obj: Dict, *args):
    """A little helper method to copy properties from one object to another."""
    for prop in args:
        value = getattr(from_obj, prop, None)
        if value is not None:
            to_obj[prop] = value


@generator_with_value
def collect_errors(stream):
    """Collect all errors from the stream into a list.

    This assumes that we have used EventErrors.add_to_event to add errors to the stream.

    Requires:
        * nothing - but if `errors` is set on an event, it will be added to the list

    Yields:
        * All the events

    Returns:
        * `datasets` - A list of dictionaries of errors.
    """

    # A dict to hold all the tables we find in the stream
    collected_errors = []

    # Iterate over the stream and collect the tables from the StartRow events
    for event in stream:
        errors = getattr(event, EventErrors.property, None)
        if errors:
            for error_entry in errors:
                error_entry = error_entry.copy()
                _populate_error_entry(
                    event,
                    error_entry,
                    "filename",
                    "r_ix",
                    "c_ix",
                    "table_name",
                    "header",
                )
                collected_errors.append(error_entry)

        yield event

    # With the stream fully consumed, we can return the collected errors
    return collected_errors
