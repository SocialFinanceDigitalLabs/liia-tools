import logging
from typing import List
import xml.etree.ElementTree as ET
import xmlschema
from xmlschema import XMLSchemaValidatorError

from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import collector, block_check
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

    If there is no content at all, then the node is not returned.
    """
    if not hasattr(event, "text"):
        return event

    if event.text is None:
        return None

    text = event.text.strip()
    if len(text) > 0:
        return event.from_event(event, text=text)
    else:
        return None


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
                category_dict["category"][i] = {**category_dict["category"][i], **name_dict}

            return category_dict

    else:
        return


@streamfilter()
def add_schema(event, schema: xmlschema.XMLSchema, schema_path: str):
    """
    Requires each event to have event.context as set by :func:`add_context`

    Based on the context (a tuple of element tags) it will set path which is the
    derived path (based on the context tags) joined by '/' and schema holding the
    corresponding schema element, if found.

    Provides: path, schema
    """
    schema_dict = None
    assert (
        event.context
    ), "This filter required event.context to be set - see add_context"
    path = "/".join(event.context)
    tag = event.context[-1]
    el = schema.get_element(tag, path)

    if el.type.name is not None and el.type.name[-4:] == "type":
        schema_dict = _create_category_dict(el.type.name, schema_path)

    return event.from_event(event, path=path, schema=el, schema_dict=schema_dict)


def _get_validation_error(schema, node) -> XMLSchemaValidatorError:
    try:
        schema.validate(node)
        return None
    except XMLSchemaValidatorError as e:
        return e


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event):
    """
    Validates each element, and if not valid, sets the properties:

    * valid - (always False)
    * validation_message - a descriptive validation message
    """
    validation_error = _get_validation_error(event.schema, event.node)
    if validation_error is None:
        return event

    message = (
        validation_error.reason
        if hasattr(validation_error, "reason")
        else validation_error.message
    )
    return events.StartElement.from_event(
        event, valid=False, validation_message=message
    )


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def prop_to_attribute(event, prop_name):
    """
    Elevates an event property to an XML attribute.
    """
    if hasattr(event, prop_name):
        attrs = getattr(event, "attrs", {})
        attrs[prop_name] = getattr(event, prop_name)
        return events.StartElement.from_event(event, attrs=attrs)
    else:
        return event


@collector(check=block_check(events.StartElement), receive_stream=True)
def remove_invalid(stream, tag_name):
    """
    Filters out events with the given tag name if they are not valid
    """
    stream = list(stream)
    first = stream[0]
    last = stream[-1]
    stream = stream[1:-1]

    if first.tag == tag_name and not getattr(first, "valid", True):
        yield from []
    else:
        yield first

        if len(stream) > 0:
            yield from remove_invalid(stream, tag_name=tag_name)

        yield last


@streamfilter(check=lambda x: True)
def counter(event, counter_check, context):
    if counter_check(event):
        context["pass"] += 1
    else:
        context["fail"] += 1
    return event
