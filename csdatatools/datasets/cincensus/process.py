import logging
from collections import defaultdict

import xmlschema
from sfdata_stream_parser import events

from csdatatools.util.stream import consume
from csdatatools.util.xml import etree, to_xml

from csdatatools.datasets.cincensus.filters import strip_text, add_context, add_schema, validate_elements, \
    prop_to_attribute, remove_invalid, counter
from csdatatools.util.xml import dom_parse

log = logging.getLogger(__name__)


def attach_schema(stream, schema: xmlschema.XMLSchema):
    """
    Add context and schema ready for validation.
    """
    stream = strip_text(stream)
    stream = add_context(stream)
    stream = add_schema(stream, schema=schema)
    return stream


def validationreport(filename, schema: xmlschema.XMLSchema, output):
    stream = dom_parse(filename)
    stream = attach_schema(stream, schema)
    stream = validate_elements(stream)
    stream = prop_to_attribute(stream, prop_name='validation_message')

    builder = etree.TreeBuilder()
    stream = to_xml(stream, builder)

    log.info("Processed %s", consume(stream))

    element = builder.close()
    element = etree.tostring(element, encoding='utf-8', pretty_print=True)
    with open(output, 'wb') as FILE:
        FILE.write(element)


def remove_invalid_children(filename, schema: xmlschema.XMLSchema, output):
    stream = dom_parse(filename)
    stream = attach_schema(stream, schema)
    stream = validate_elements(stream)

    counter_context = defaultdict(lambda: 0)
    stream = counter(stream,
                     counter_check=lambda e: isinstance(e, events.StartElement) and not getattr(e, 'valid', True),
                     context=counter_context)
    stream = remove_invalid(stream, tag_name="Child")

    builder = etree.TreeBuilder()
    stream = to_xml(stream, builder)

    log.info("Processed %s", consume(stream))
    log.info("Removed %d invalid elements", counter_context['pass'])

    element = builder.close()
    element = etree.tostring(element, encoding='utf-8', pretty_print=True)
    with open(output, 'wb') as FILE:
        FILE.write(element)