import logging
import xmlschema
import tablib
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from io import BytesIO, StringIO
from typing import Iterable, Union, Any, Dict, List
from pathlib import Path
from tablib import import_book, import_set, UnsupportedFormat

from sfdata_stream_parser import events, collectors
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import (
    generator_with_value,
    pass_event,
    streamfilter,
)

from liiatools.common.data import FileLocator
from liiatools.common.stream_errors import StreamError, EventErrors
from liiatools.common.converters import (
    to_date,
    to_numeric,
    to_postcode,
    to_regex,
    to_category,
    to_time,
)

from .spec.__data_schema import Column, DataSchema, Numeric, Category

logger = logging.getLogger(__name__)


def _import_set_workaround(data):
    """
    Workaround for a bug in tablib that causes it to fail to import
    sets of data.
    """
    try:
        return import_set(data)
    except UnsupportedFormat:
        return pd.read_csv(data)


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
        dataset = _import_set_workaround(data)
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


def _pandas_dataframe_to_stream(dataset: pd.DataFrame, **kwargs):
    params = {k: v for k, v in kwargs.items() if v is not None}
    yield events.StartContainer(**params)
    yield events.StartTable(headers=dataset.columns.tolist())
    for r_ix, row in enumerate(dataset.itertuples(index=False)):
        yield events.StartRow()
        for c_ix, cell in enumerate(row[0:]):
            if isinstance(cell, float):
                if np.isnan(cell):
                    cell = ""
            yield events.Cell(
                r_ix=r_ix,
                c_ix=c_ix,
                header=dataset.columns.tolist()[c_ix],
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

    elif isinstance(data, pd.DataFrame):
        yield from _pandas_dataframe_to_stream(data, filename=filename)


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
        if all(header == "" for header in headers):
            return EventErrors.add_to_event(
                event,
                type="BlankHeaders",
                message=f"Could not identify headers as first row is blank",
            )
        table_name = schema.get_table_from_headers(event.headers)

    if table_name:
        return event.from_event(
            event, table_name=table_name, table_spec=schema.column_map[table_name]
        )
    else:
        return EventErrors.add_to_event(
            event,
            type="UnidentifiedTable",
            message=f"Failed to identify table based on headers, actual headers are {headers}",
        )


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


@streamfilter(
    check=type_check((events.Cell, events.TextNode)), fail_function=pass_event
)
def log_blanks(event):
    """Creates a EventErrors for cells flagged as not allowing blank but that are empty (None or blank string)."""
    column_spec = getattr(event, "column_spec", None)

    # No specification - we can't check
    if not column_spec:
        return event

    # Not required - we don't need to check
    if column_spec.canbeblank:
        return event

    cell_value = getattr(event, "cell", None)
    if isinstance(cell_value, str):
        cell_value = cell_value.strip()

    if cell_value is None:
        cell_value = ""

    if cell_value == "":
        return EventErrors.add_to_event(
            event, type="Blank", message=f"Blank value for mandatory cell"
        )

    return event


@streamfilter(
    check=type_check((events.Cell, events.TextNode)), fail_function=pass_event
)
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
    elif column_spec.type == "time":
        converter = lambda x: to_time(x, column_spec.time)
    else:
        return EventErrors.add_to_event(
            event, type="UnknownType", message=f"Unknown cell type {column_spec.type}"
        )

    cell_value = getattr(event, "cell", None)

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


@streamfilter(check=type_check(events.TextNode), fail_function=pass_event)
def strip_text(event):
    """
    Strips surrounding whitespaces from :class:`sfdata_stream_parser.events.TextNode`. If the event does
    not have a text property then this filter fails silently.

    :param event: A filtered list of event objects
    :return: Event with whitespace striped
    """
    if not hasattr(event, "cell"):
        return event

    if event.cell is None:
        return event

    cell = event.cell.strip()
    if len(cell) > 0:
        return event.from_event(event, cell=cell)
    else:
        return None


@streamfilter(default_args=lambda: {"context": []})
def add_context(event, context: List[str]):
    """
    Adds 'context' to XML structures. For each :class:`sfdata_stream_parser.events.StartElement` the tag name is
    added to a 'context' tuple, and for each :class:`sfdata_stream_parser.events.EndElement` the context is popped.

    For all other events, the context tuple is set as-is.

    :param event: A filtered list of event objects
    :param context: A list to be populated with context information
    :return: Event with context
    """
    if isinstance(event, events.StartElement):
        context.append(event.tag)
        local_context = tuple(context)
    elif isinstance(event, events.EndElement):
        local_context = tuple(context)
        context.pop()
    else:
        local_context = tuple(context)

    return event.from_event(event, context=local_context)


@streamfilter()
def add_schema(event, schema: xmlschema.XMLSchema):
    """
    Requires each event to have event.context as set by :func:`add_context`

    Based on the context (a tuple of element tags) it will set path which is the
    derived path (based on the context tags) joined by '/' and schema holding the
    corresponding schema element, if found.

    :param event: A filtered list of event objects
    :param schema: The xml schema to be attached to a given event
    :return: Event with path, schema and header attributes
    """
    assert (
        event.context
    ), "This filter required event.context to be set - see add_context"
    path = "/".join(event.context)
    tag = event.context[-1]
    el = schema.get_element(tag, path)
    header = getattr(el, "name", None)
    return event.from_event(event, path=path, schema=el, header=header)


def _get_validation_error(event, schema, node):
    """
    Validate an event

    :param event: A filtered list of event objects
    :param schema: The xml schema attached to a given event
    :param node: The node attached to a given event
    :return: Error information
    """
    try:
        validation_error_iterator = schema.iter_errors(node)
        for validation_error in validation_error_iterator:
            if " expected" in validation_error.reason:
                raise ValueError(
                    f"Missing required field: '{validation_error.particle.name}' which occurs in the node starting on "
                    f"line: {validation_error.sourceline}"
                )

    except AttributeError:  # Raised for nodes that don't exist in the schema
        raise ValueError(f"Unexpected node '{event.tag}'")


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event):
    """
    Validates each element, and if not valid raises ValidationError:

    :param event: A filtered list of event objects
    :return: Event if valid or event and error message if invalid
    """
    # Only validate root element and elements with no schema
    if isinstance(event, events.StartElement) and (
        event.node.getparent() is None or event.schema is None
    ):
        try:
            _get_validation_error(event, event.schema, event.node)
            return event
        except ValueError as e:
            return EventErrors.add_to_event(
                event, type="ValidationError", message="Invalid node", exception=str(e)
            )
    else:
        return event


def _create_category_spec(field: str, file: Path) -> List[Category] | None:
    """
    Create a list of Category classes containing the different categorical values of a given field to conform categories
    e.g. [Category(code='0', name='Not an Agency Worker'), Category(code='1', name='Agency Worker')]

    :param field: Name of the categorical field you want to find the values for
    :param file: Path to the .xsd schema containing possible categories
    :return: List of Category classes of categorical values and potential alternatives
    """
    category_spec = []

    xsd_xml = ET.parse(file)
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"
    element = xsd_xml.find(search_elem)

    if element is not None:
        search_value = f".//{{http://www.w3.org/2001/XMLSchema}}enumeration"  # Find the 'code' parameter
        value = element.findall(search_value)
        if value:
            for v in value:
                category_spec.append(Category(code=v.get("value")))

            search_doc = f".//{{http://www.w3.org/2001/XMLSchema}}documentation"  # Find the 'name' parameter
            documentation = element.findall(search_doc)
            for i, d in enumerate(documentation):
                category_spec[i].name = d.text
            return category_spec
    else:
        return


def _create_numeric_spec(field: str, file: Path) -> Numeric:
    """
    Create a Numeric class containing the different numeric parameters of a given field to conform numbers
    e.g. Numeric(type='float', min_value=0, max_value=1, decimal_places=6)

    :param field: Name of the numeric field you want to find the parameters for
    :param file: Path to the .xsd schema containing possible numeric parameters
    :return: Numeric class of numeric parameters
    """
    numeric_spec = None

    xsd_xml = ET.parse(file)
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"
    element = xsd_xml.find(search_elem)

    search_restriction = f".//{{http://www.w3.org/2001/XMLSchema}}restriction"  # Find the 'type' parameter
    restriction = element.findall(search_restriction)
    for r in restriction:
        if r.get("base")[3:] == "decimal":
            numeric_spec = Numeric(type="float")
        elif r.get("base")[3:] == "integer":
            numeric_spec = Numeric(type="integer")

    search_fraction_digits = f".//{{http://www.w3.org/2001/XMLSchema}}fractionDigits"  # Find the 'decimal' parameter
    fraction_digits = element.findall(search_fraction_digits)
    for f in fraction_digits:
        numeric_spec.decimal_places = int(f.get("value"))

    search_min_inclusive = f".//{{http://www.w3.org/2001/XMLSchema}}minInclusive"  # Find the 'min_value' parameter
    min_inclusive = element.findall(search_min_inclusive)
    for m in min_inclusive:
        numeric_spec.min_value = int(m.get("value"))

    search_max_inclusive = f".//{{http://www.w3.org/2001/XMLSchema}}maxInclusive"  # Find the 'max_value' parameter
    max_inclusive = element.findall(search_max_inclusive)
    for m in max_inclusive:
        numeric_spec.max_value = int(m.get("value"))

    return numeric_spec


def _create_regex_spec(field: str, file: Path) -> str | None:
    """
    Parse an XML file and extract the regex pattern for a given field name

    :param field: The name of the field to look for in the XML file
    :param file: The path to the XML file
    :return: The regex pattern, or None if no pattern is found
    """
    regex_spec = None

    xsd_xml = ET.parse(file)
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"
    element = xsd_xml.find(search_elem)

    search_pattern = f".//{{http://www.w3.org/2001/XMLSchema}}pattern"  # Find the 'cell_regex' parameter
    pattern = element.findall(search_pattern)
    for p in pattern:
        regex_spec = p.get("value")

    return regex_spec
