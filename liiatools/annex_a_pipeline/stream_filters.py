import logging

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import (
    pass_event,
    streamfilter,
)

from liiatools.common.spec.__data_schema import Column, DataSchema

log = logging.getLogger(__name__)


@streamfilter(check=type_check(events.Cell), fail_function=pass_event)
def convert_column_header_to_match(event, schema: DataSchema):
    """
    Converts the column header to the correct column header it was matched with e.g. Age -> Age of Child (Years)
    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    if hasattr(event, "table_name") and hasattr(event, "header"):
        column_config = schema.table.get(event.table_name)
        for column in column_config:
            if column_config[column].header_regex is not None:
                for regex in column_config[column].header_regex:
                    parse = Column().parse_regex(regex)
                    if parse.match(event.header) is not None:
                        return event.from_event(event, header=column)
    return event
