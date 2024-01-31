import logging
from pathlib import Path

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import pass_event, streamfilter

from liiatools.common.spec.__data_schema import Column, Numeric
from liiatools.common.stream_filters import (
    _create_category_spec,
    _create_regex_spec,
    _create_numeric_spec,
)

logger = logging.getLogger(__name__)


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def add_column_spec(event, schema_path: Path):
    """
    Add a Column class containing schema attributes to an event object based on its type and occurrence

    :param event: An event object with a schema attribute
    :param schema_path: The path to the schema file
    :return: A new event object with a column_spec attribute, or the original event object if no schema is found
    """
    column_spec = Column()

    if event.schema.occurs[0] == 1:
        column_spec.canbeblank = False

    config_type = event.schema.type.name
    if config_type is not None:
        if config_type[-4:] == "type":
            column_spec.category = _create_category_spec(config_type, schema_path)
        if config_type in ["positiveintegertype"]:
            column_spec.numeric = _create_numeric_spec(config_type, schema_path)
        if config_type in ["upntype"]:
            column_spec.string = "regex"
            column_spec.cell_regex = _create_regex_spec(config_type, schema_path)
        if config_type == "{http://www.w3.org/2001/XMLSchema}date":
            column_spec.date = "%Y-%m-%d"
        if config_type == "{http://www.w3.org/2001/XMLSchema}dateTime":
            column_spec.date = "%Y-%m-%dT%H:%M:%S"
        if config_type in [
            "{http://www.w3.org/2001/XMLSchema}integer",
            "{http://www.w3.org/2001/XMLSchema}gYear",
        ]:
            column_spec.numeric = Numeric(type="integer")
        if config_type == "{http://www.w3.org/2001/XMLSchema}string":
            column_spec.string = "alphanumeric"
    else:
        column_spec.string = "alphanumeric"

    return event.from_event(event, column_spec=column_spec)
