import logging

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import and_check, type_check
from sfdata_stream_parser.filters.generic import pass_event, streamfilter

from ....spec.s903 import DataSchema
from ...shared_functions.common import check_postcode
from ...shared_functions.converters import to_date
from .converters import to_category, to_integer

log = logging.getLogger(__name__)


@streamfilter(check=type_check(events.StartTable), fail_function=pass_event)
def add_table_name(event, schema: DataSchema):
    """
    Match the loaded table name against one of the 10 903 file names

    :param event: A filtered list of event objects of type StartTable
    :return: An updated list of event objects
    """
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

    :param event: A filtered list of event objects of type Cell
    :param config: The loaded configuration to use
    :return: An updated list of event objects
    """
    if hasattr(event, "table_name"):
        table_config = schema.table[event.table_name]
        if event.header in table_config:
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
def clean_categories(event):
    """
    Convert all values that should be categories to categories based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    text = to_category(event.cell, event.column_spec)
    if text is not None:
        return event.from_event(event, cell=text, error="0")
    else:
        return event.from_event(event, cell="", error="1")


@streamfilter(check=cell_type_check("integer"), fail_function=pass_event)
def clean_integers(event):
    """
    Convert all values that should be integers into integers based on the config.yaml file

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    text = to_integer(event.cell)
    return event.from_event(event, cell=text, error="0")


@streamfilter(check=cell_type_check("postcode"), fail_function=pass_event)
def clean_postcodes(event):
    """
    Check that all values that should be postcodes are postcodes

    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    error = "0"
    text = ""
    try:
        text = check_postcode(event.cell)
    except (AttributeError, TypeError, ValueError):
        error = "1"
    return event.from_event(event, cell=text, error=error)
