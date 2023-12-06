from collections import namedtuple
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import Iterable

from sfdata_stream_parser.events import StartElement, EndElement, TextNode, ParseEvent
from liiatools.csww_pipeline.stream_parse import dom_parse
from liiatools.common.spec.__data_schema import (
    Column,
    Numeric,
    Category,
)
from liiatools.csww_pipeline.spec import (
    load_schema,
    load_schema_path,
)
from liiatools.csww_pipeline.spec.samples import CSWW_2022
from liiatools.csww_pipeline.stream_filters import (
    strip_text,
    add_context,
    add_schema,
    validate_elements,
    _create_category_spec,
    _create_numeric_spec,
    _create_regex_spec,
    add_column_spec,
)


def test_strip_text():
    stream = [
        TextNode(text=None),
        TextNode(cell=None, text=None),
        TextNode(cell="string", text=None),
        TextNode(cell=" string_with_whitespace ", text=None),
    ]

    stripped_stream = list(strip_text(stream))
    assert stream[0] == stripped_stream[0]
    assert stream[1] == stripped_stream[1]
    assert stripped_stream[2].cell == "string"
    assert stripped_stream[3].cell == "string_with_whitespace"


def test_add_context():
    stream = [
        StartElement(tag="Message"),
        StartElement(tag="Header"),
        TextNode(cell="string", text=None),
        EndElement(tag="Header"),
        EndElement(tag="Message"),
    ]

    context_stream = list(add_context(stream))
    assert context_stream[0].context == ("Message",)
    assert context_stream[1].context == ("Message", "Header")
    assert context_stream[2].context == ("Message", "Header")
    assert context_stream[3].context == ("Message", "Header")
    assert context_stream[4].context == ("Message",)


def test_add_schema():
    schema = load_schema(year=2022)
    stream = [
        StartElement(tag="Message", context=("Message",)),
        StartElement(tag="Header", context=("Message", "Header")),
        TextNode(cell="string", text=None, context=("Message", "Header")),
        EndElement(tag="Header", context=("Message", "Header")),
        EndElement(tag="Message", context=("Message",)),
    ]

    schema_stream = list(add_schema(stream, schema=schema))

    assert schema_stream[0].schema.name == "Message"
    assert schema_stream[0].schema.occurs == (1, 1)
    assert schema_stream[1].schema.name == "Header"
    assert schema_stream[1].schema.occurs == (0, 1)
    assert schema_stream[2].schema.name == "Header"
    assert schema_stream[2].schema.occurs == (0, 1)
    assert schema_stream[3].schema.name == "Header"
    assert schema_stream[3].schema.occurs == (0, 1)
    assert schema_stream[4].schema.name == "Message"
    assert schema_stream[4].schema.occurs == (1, 1)


def _xml_to_stream(root) -> Iterable[ParseEvent]:
    schema = load_schema(2022)

    input = BytesIO(ET.tostring(root, encoding="utf-8"))
    stream = dom_parse(input, filename="test.xml")
    stream = strip_text(stream)
    stream = add_context(stream)
    stream = add_schema(stream, schema=schema)
    stream = validate_elements(stream)
    return list(stream)


def test_validate_all_valid():
    with CSWW_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    stream = _xml_to_stream(root)

    for event in stream:
        assert not hasattr(event, "errors")


def test_validate_missing_required_field():
    with CSWW_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    parent = root.find(".//CSWWWorker")
    el = parent.find("AgencyWorker")
    parent.remove(el)

    stream = _xml_to_stream(root)

    errors = []
    for event in stream:
        if hasattr(event, "errors"):
            errors.append(event.errors)

    assert list(errors[0])[0] == {
        "type": "ValidationError",
        "message": "Invalid node",
        "exception": "Missing required field: 'AgencyWorker' which occurs in the node starting on line: 20",
    }


def test_validate_reordered_required_field():
    with CSWW_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    el_parent = root.find(".//AgencyWorker/..")
    el_child_id = el_parent.find("AgencyWorker")
    el_parent.remove(el_child_id)
    el_parent.append(el_child_id)

    stream = _xml_to_stream(root)

    errors = []
    for event in stream:
        if hasattr(event, "errors"):
            errors.append(event.errors)

    assert list(errors[0])[0] == {
        "type": "ValidationError",
        "message": "Invalid node",
        "exception": "Missing required field: 'AgencyWorker' which occurs in the node starting on line: 20",
    }


def test_validate_unexpected_node():
    with CSWW_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    parent = root.find(".//CSWWWorker")
    ET.SubElement(parent, "Unknown_Node")

    stream = _xml_to_stream(root)

    errors = []
    for event in stream:
        if hasattr(event, "errors"):
            errors.append(event.errors)

    assert list(errors[0])[0] == {
        "type": "ValidationError",
        "message": "Invalid node",
        "exception": "Unexpected node 'Unknown_Node'",
    }


def test_create_category_spec():
    schema_path = load_schema_path(2022)
    field = "agencyworkertype"
    category_spec = _create_category_spec(field, schema_path)

    assert category_spec == [
        Category(
            code="0",
            name="Not an Agency Worker",
            cell_regex=None,
            model_config={"extra": "forbid"},
        ),
        Category(
            code="1",
            name="Agency Worker",
            cell_regex=None,
            model_config={"extra": "forbid"},
        ),
    ]


def test_create_numeric_spec():
    schema_path = load_schema_path(2022)
    field = "twodecimalplaces"
    numeric_spec = _create_numeric_spec(field, schema_path)

    assert numeric_spec == Numeric(
        type="float",
        min_value=None,
        max_value=None,
        decimal_places=2,
        model_config={"extra": "forbid"},
    )


def test_create_regex_spec():
    schema_path = load_schema_path(2022)
    field = "swetype"
    regex_spec = _create_regex_spec(field, schema_path)

    assert regex_spec == r"[A-Za-z]{2}\d{10}"


def test_add_column_spec():
    Schema = namedtuple("schema", "occurs type")
    Name = namedtuple("type", "name")

    category_schema = Schema((0, 1), Name("agencyworkertype"))
    numeric_schema = Schema((0, 1), Name("onedecimalplace"))
    regex_schema = Schema((0, 1), Name("swetype"))
    date_schema = Schema((0, 1), Name("{http://www.w3.org/2001/XMLSchema}date"))
    integer_schema = Schema((0, 1), Name("http://www.w3.org/2001/XMLSchema}integer"))
    string_schema = Schema((0, 1), Name("{http://www.w3.org/2001/XMLSchema}string"))
    alphanumeric_schema = Schema((1, 1), Name(None))

    schema_path = load_schema_path(2022)
    stream = [
        TextNode(text=None, schema=category_schema),
        TextNode(text=None, schema=numeric_schema),
        TextNode(text=None, schema=regex_schema),
        TextNode(text=None, schema=date_schema),
        TextNode(text=None, schema=integer_schema),
        TextNode(text=None, schema=string_schema),
        TextNode(text=None, schema=alphanumeric_schema),
    ]

    column_spec = list(add_column_spec(stream, schema_path=schema_path))

    assert column_spec[0].column_spec == Column(
        string=None,
        numeric=None,
        date=None,
        dictionary=None,
        category=[
            Category(
                code="0",
                name="Not an Agency Worker",
                cell_regex=None,
                model_config={"extra": "forbid"},
            ),
            Category(
                code="1",
                name="Agency Worker",
                cell_regex=None,
                model_config={"extra": "forbid"},
            ),
        ],
        header_regex=None,
        cell_regex=None,
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[1].column_spec == Column(
        string=None,
        numeric=Numeric(
            type="float",
            min_value=None,
            max_value=None,
            decimal_places=1,
            model_config={"extra": "forbid"},
        ),
        date=None,
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex=None,
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[2].column_spec == Column(
        string="regex",
        numeric=None,
        date=None,
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex="[A-Za-z]{2}\\d{10}",
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[3].column_spec == Column(
        string=None,
        numeric=None,
        date="%Y-%m-%d",
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex=None,
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[4].column_spec == Column(
        string=None,
        numeric=None,
        date=None,
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex=None,
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[5].column_spec == Column(
        string="alphanumeric",
        numeric=None,
        date=None,
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex=None,
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[6].column_spec == Column(
        string="alphanumeric",
        numeric=None,
        date=None,
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex=None,
        canbeblank=False,
        model_config={"extra": "forbid"},
    )
