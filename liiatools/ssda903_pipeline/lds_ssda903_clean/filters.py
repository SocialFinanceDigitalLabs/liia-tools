import logging
from typing import Dict, List

from sfdata_stream_parser import collectors, events
from sfdata_stream_parser.checks import and_check, type_check
from sfdata_stream_parser.filters.generic import (
    generator_with_value,
    pass_event,
    streamfilter,
)

from liiatools.datasets.shared_functions.converters import (
    to_date,
    to_integer,
    to_postcode,
)

from ..spec import Column, DataSchema
from .converters import to_category

log = logging.getLogger(__name__)


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


def cell_type_check(type_name: str):
    def _check(event: events.ParseEvent) -> bool:
        if not hasattr(event, "column_spec"):
            return False
        return event.column_spec.type == type_name

    return and_check(type_check(events.Cell), _check)


@streamfilter(check=cell_type_check("date"), fail_function=pass_event)
def clean_dates(event):
    """
    Convert all values that should be dates to dates based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """

    try:
        text = to_date(event.cell, event.column_spec.date)
        return event.from_event(event, cell=text, error="0")
    except (AttributeError, TypeError, ValueError):
        return event.from_event(event, cell="", error="1")


@streamfilter(check=cell_type_check("category"), fail_function=pass_event)
def clean_categories(event, preserve_value=False):
    """
    Convert all values that should be categories to categories based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    cell_value = getattr(event, "cell", None)

    try:
        cell_value = to_category(cell_value, event.column_spec)
        return event.from_event(event, cell=cell_value, error="0")
    except ValueError:
        return event.from_event(
            event, cell=event.cell if preserve_value else "", error="1"
        )


@streamfilter(check=cell_type_check("integer"), fail_function=pass_event)
def clean_integers(event, preserve_value=False):
    """
    Convert all values that should be integers into integers based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    cell_value = getattr(event, "cell", None)
    try:
        cell_value = to_integer(cell_value)
        return event.from_event(event, cell=cell_value, error="0")
    except ValueError:
        return event.from_event(
            event, cell=event.cell if preserve_value else "", error="1"
        )


@streamfilter(check=cell_type_check("postcode"), fail_function=pass_event)
def clean_postcodes(event, preserve_value=False):
    """
    Check that all values that should be postcodes are postcodes

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    cell_value = getattr(event, "cell", None)
    try:
        cell_value = to_postcode(cell_value)
        return event.from_event(event, cell=cell_value, error="0")
    except ValueError:
        return event.from_event(
            event, cell=event.cell if preserve_value else "", error="1"
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
    dataset: Dict[str, List] = {}

    # Iterate over the stream and collect the tables from the StartRow events
    for event in stream:
        if isinstance(event, events.StartRow) and hasattr(event, "table_name"):
            table_data = dataset.setdefault(event.table_name, [])
            table_data.append(event.row_values)

        yield event

    # With the stream fully consumed, we can return the populated dataset
    return dataset
