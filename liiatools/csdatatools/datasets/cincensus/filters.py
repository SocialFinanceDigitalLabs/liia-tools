import logging
from typing import List

import xmlschema
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import collector, block_check
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from xmlschema import XMLSchemaValidatorError

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
