import sys
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import Iterable

import yaml
from sfdata_stream_parser.events import ParseEvent, StartElement
from sfdata_stream_parser.parser.xml import parse
from xmlschema.validators.exceptions import XMLSchemaValidatorError
from xmlschema.validators.facets import XsdMinLengthFacet
from xmlschema.validators.groups import XsdGroup

from liiatools.cin_census_pipeline import stream_filters as filters
from liiatools.cin_census_pipeline.spec import load_schema
from liiatools.cin_census_pipeline.spec.samples import CIN_2022
from liiatools.cin_census_pipeline.spec.samples import DIR as SAMPLES_DIR
from liiatools.datasets.cin_census.lds_cin_clean import validator
from liiatools.datasets.cin_census.lds_cin_clean.parse import dom_parse


def _xml_to_stream(root, validate=True, new_validator=True) -> Iterable[ParseEvent]:
    schema = load_schema(2022)

    input = BytesIO(ET.tostring(root, encoding="utf-8"))
    stream = dom_parse(input)
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_schema(stream, schema=schema)
    if validate:
        if new_validator:
            stream = filters.validate_elements(stream)
        else:
            stream = validator.validate_elements(
                stream, LAchildID_error=[], field_error=[]
            )

    return list(stream)


def test_validate_all_valid():
    with CIN_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    stream = _xml_to_stream(root)

    # Count nodes in stream that are not valid
    invalid_nodes = [e for e in stream if not getattr(e, "valid", True)]
    assert len(invalid_nodes) == 0


def test_validate_missing_child_id():
    with CIN_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    parent = root.find(".//ChildIdentifiers")
    el = parent.find("LAchildID")
    parent.remove(el)

    stream = _xml_to_stream(root)

    # Count nodes in stream that are not valid
    invalid_nodes = [e for e in stream if not getattr(e, "valid", True)]
    assert len(invalid_nodes) == 1

    error: XMLSchemaValidatorError = invalid_nodes[0].validation_errors[0]
    assert type(error.validator) == XsdGroup
    assert error.particle.name == "LAchildID"
    assert error.occurs == 0
    assert error.sourceline == 19


def test_validate_blank_child_id():
    with CIN_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    el = root.find(".//LAchildID")
    el.text = ""
    stream = _xml_to_stream(root)

    # Count nodes in stream that are not valid
    invalid_nodes = [e for e in stream if not getattr(e, "valid", True)]
    assert len(invalid_nodes) == 1

    error: XMLSchemaValidatorError = invalid_nodes[0].validation_errors[0]
    assert type(error.validator) == XsdMinLengthFacet
    assert error.reason == "value length cannot be lesser than 1"
    assert error.sourceline == 20


def test_validate_reordered_child_id():
    with CIN_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    el_parent = root.find(".//LAchildID/..")
    el_child_id = el_parent.find("LAchildID")
    el_parent.remove(el_child_id)
    el_parent.append(el_child_id)

    xml = ET.tostring(el_parent, encoding="utf-8")
    stream = _xml_to_stream(root)

    # Count nodes in stream that are not valid
    invalid_nodes = [e for e in stream if not getattr(e, "valid", True)]
    assert len(invalid_nodes) == 1

    error: XMLSchemaValidatorError = invalid_nodes[0].validation_errors[0]
    assert type(error.validator) == XsdGroup
    assert error.particle.name == "LAchildID"
    assert error.occurs == 0
    assert error.sourceline == 19
