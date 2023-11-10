import logging
from typing import List
import xml.etree.ElementTree as ET
import xmlschema

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


@streamfilter(default_args=lambda: {"context": []})
def add_context(event, context: List[str]):
    """
    Adds 'context' to XML structures. For each :class:`sfdata_stream_parser.events.StartElement` the tag name is
    added to a 'context' tuple, and for each :class:`sfdata_stream_parser.events.EndElement` the context is popped.

    For all other events, the context tuple is set as-is.

    Provides: context
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


@streamfilter(check=type_check(events.TextNode), fail_function=pass_event)
def strip_text(event):
    """
    Strips surrounding whitespaces from :class:`sfdata_stream_parser.events.TextNode`. If the event does
    not have a text property then this filter fails silently.
    """
    if not hasattr(event, "text"):
        return event

    if event.text is None:
        return event

    text = event.text.strip()
    return event.from_event(event, text=text)


def _create_category_dict(field: str, file: str):
    """
    Create a dictionary containing the different categorical values of a given field to conform categories
    e.g. {'category': [{'code': '0', 'name': 'Not an Agency Worker'}, {'code': '1', 'name': 'Agency Worker'}]}

    :param field: Name of the categorical field you want to find the values for
    :param file: Path to the .xsd schema containing possible categories
    :return: Dictionary of categorical values and potential alternatives
    """
    category_dict = {"category": []}

    xsd_xml = ET.parse(file)
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"
    element = xsd_xml.find(search_elem)

    if element is not None:
        search_value = f".//{{http://www.w3.org/2001/XMLSchema}}enumeration"
        value = element.findall(search_value)
        if value:
            for v in value:
                code_dict = {"code": v.get("value")}
                category_dict["category"].append(code_dict)

            search_doc = f".//{{http://www.w3.org/2001/XMLSchema}}documentation"
            documentation = element.findall(search_doc)
            for i, d in enumerate(documentation):
                name_dict = {"name": d.text}
                category_dict["category"][i] = {
                    **category_dict["category"][i],
                    **name_dict,
                }

            return category_dict

    else:
        return


def _create_float_dict(field: str, file: str):
    """
    Create a dictionary containing the different float parameters of a given field to conform floats
    e.g. {'numeric': 'decimal', 'fixed': 'true', 'decimal': '6', 'min_inclusive': '0', 'max_inclusive': '1'}

    :param field: Name of the float field you want to find the parameters for
    :param file: Path to the .xsd schema containing possible float parameters
    :return: Dictionary of float parameters
    """
    float_dict = None

    xsd_xml = ET.parse(file)
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"
    element = xsd_xml.find(search_elem)

    search_restriction = f".//{{http://www.w3.org/2001/XMLSchema}}restriction"
    restriction = element.findall(search_restriction)
    for r in restriction:
        code_dict = {
            "numeric": r.get("base")[3:]
        }  # Remove the "xs:" from the start of the base string
        if code_dict["numeric"] == "decimal":
            float_dict = code_dict

    search_fraction_digits = f".//{{http://www.w3.org/2001/XMLSchema}}fractionDigits"
    fraction_digits = element.findall(search_fraction_digits)
    for f in fraction_digits:
        fraction_digits_dict = {"fixed": f.get("fixed"), "decimal": f.get("value")}
        float_dict = {**float_dict, **fraction_digits_dict}

    search_min_inclusive = f".//{{http://www.w3.org/2001/XMLSchema}}minInclusive"
    min_inclusive = element.findall(search_min_inclusive)
    for m in min_inclusive:
        min_dict = {"min_inclusive": m.get("value")}
        float_dict = {**float_dict, **min_dict}

    search_max_inclusive = f".//{{http://www.w3.org/2001/XMLSchema}}maxInclusive"
    max_inclusive = element.findall(search_max_inclusive)
    for m in max_inclusive:
        max_dict = {"max_inclusive": m.get("value")}
        float_dict = {**float_dict, **max_dict}

    return float_dict


def _create_regex_dict(field: str, file: str):
    """
    Parse an XML file and extract the regex pattern for a given field name

    :param field: The name of the field to look for in the XML file
    :param file: The path to the XML file
    :return: A dictionary with the key "regex_string" and the value as the regex pattern, or None if no pattern is found
    """
    regex_dict = None

    xsd_xml = ET.parse(file)
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"
    element = xsd_xml.find(search_elem)

    search_restriction = f".//{{http://www.w3.org/2001/XMLSchema}}restriction"
    restriction = element.findall(search_restriction)
    for r in restriction:
        if r.get("base") == "xs:string":
            regex_dict = {"regex_string": None}

        search_pattern = f".//{{http://www.w3.org/2001/XMLSchema}}pattern"
        pattern = element.findall(search_pattern)
        for p in pattern:
            regex_dict["regex_string"] = p.get("value")

    return regex_dict


@streamfilter()
def add_schema(event, schema: xmlschema.XMLSchema):
    """
    Requires each event to have event.context as set by :func:`add_context`

    Based on the context (a tuple of element tags) it will set path which is the
    derived path (based on the context tags) joined by '/' and schema holding the
    corresponding schema element, if found.

    Provides: path, schema
    """
    assert (
        event.context
    ), "This filter required event.context to be set - see add_context"
    path = "/".join(event.context)
    tag = event.context[-1]
    el = schema.get_element(tag, path)

    return event.from_event(event, path=path, schema=el)


@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)
def add_schema_dict(event, schema_path: str):
    """
    Add a dictionary of schema attributes to an event object based on its type and occurrence

    :param event: An event object with a schema attribute
    :param schema_path: The path to the schema file
    :return: A new event object with a schema_dict attribute, or the original event object if no schema_dict is found
    """
    schema_dict = None

    config_type = event.schema.type.name
    if config_type is not None:
        if config_type[-4:] == "type":
            schema_dict = _create_category_dict(config_type, schema_path)
        if config_type in ["onedecimalplace", "twodecimalplaces", "ftetype"]:
            schema_dict = _create_float_dict(config_type, schema_path)
        if config_type in ["swetype"]:
            schema_dict = _create_regex_dict(config_type, schema_path)
        if config_type == "{http://www.w3.org/2001/XMLSchema}date":
            schema_dict = {"date": "%Y-%m-%d"}
        if config_type == "{http://www.w3.org/2001/XMLSchema}integer":
            schema_dict = {"numeric": "integer"}
        if config_type == "{http://www.w3.org/2001/XMLSchema}string":
            schema_dict = {"string": "alphanumeric"}

        if schema_dict is not None:
            if event.schema.occurs[0] == 0:
                schema_dict = {**schema_dict, **{"canbeblank": True}}
            elif event.schema.occurs[0] == 1:
                schema_dict = {**schema_dict, **{"canbeblank": False}}

    return event.from_event(event, schema_dict=schema_dict)
