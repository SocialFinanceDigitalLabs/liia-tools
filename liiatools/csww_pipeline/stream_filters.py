import logging
from typing import List
from pathlib import Path
import xml.etree.ElementTree as ET
import xmlschema

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import pass_event, streamfilter

from liiatools.common.stream_errors import EventErrors
from liiatools.common.spec.__data_schema import Column, Numeric, Category

logger = logging.getLogger(__name__)


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
    if isinstance(event, events.StartElement) and (event.node.getparent() is None or event.schema is None):
        try:
            _get_validation_error(event, event.schema, event.node)
            return event
        except ValueError as e:
            return EventErrors.add_to_event(
                event, type="ValidationError", message=f"Invalid node", exception=str(e)
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
        if config_type in ["onedecimalplace", "twodecimalplaces", "ftetype"]:
            column_spec.numeric = _create_numeric_spec(config_type, schema_path)
        if config_type in ["swetype"]:
            column_spec.string = "regex"
            column_spec.cell_regex = _create_regex_spec(config_type, schema_path)
        if (
            config_type == "{http://www.w3.org/2001/XMLSchema}date"
        ):
            column_spec.date = "%Y-%m-%d"
        if (
            config_type == "{http://www.w3.org/2001/XMLSchema}dateTime"
        ):
            column_spec.date = "%Y-%m-%dT%H:%M:%SZ"
        if (
            config_type == "{http://www.w3.org/2001/XMLSchema}integer"
            or config_type == "{http://www.w3.org/2001/XMLSchema}gYear"
        ):
            column_spec.numeric = Numeric(type="integer")
        if config_type == "{http://www.w3.org/2001/XMLSchema}string":
            column_spec.string = "alphanumeric"
    else:
        column_spec.string = "alphanumeric"

    return event.from_event(event, column_spec=column_spec)
