from collections import namedtuple

from sfdata_stream_parser.events import TextNode
from liiatools.common.spec.__data_schema import (
    Column,
    Numeric,
    Category,
)
from liiatools.cin_census_pipeline.spec import load_schema_path
from liiatools.cin_census_pipeline.stream_filters import add_column_spec


def test_add_column_spec():
    Schema = namedtuple("schema", "occurs type")
    Name = namedtuple("type", "name")

    category_schema = Schema((0, 1), Name("yesnotype"))
    numeric_schema = Schema((0, 1), Name("positiveintegertype"))
    regex_schema = Schema((0, 1), Name("upntype"))
    date_schema = Schema((0, 1), Name("{http://www.w3.org/2001/XMLSchema}date"))
    date_time_schema = Schema(
        (0, 1), Name("{http://www.w3.org/2001/XMLSchema}dateTime")
    )
    integer_schema = Schema((0, 1), Name("http://www.w3.org/2001/XMLSchema}integer"))
    string_schema = Schema((0, 1), Name("{http://www.w3.org/2001/XMLSchema}string"))
    alphanumeric_schema = Schema((1, 1), Name(None))

    schema_path = load_schema_path(2022)
    stream = [
        TextNode(text=None, schema=category_schema),
        TextNode(text=None, schema=numeric_schema),
        TextNode(text=None, schema=regex_schema),
        TextNode(text=None, schema=date_schema),
        TextNode(text=None, schema=date_time_schema),
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
                name="False",
                cell_regex=None,
                model_config={"extra": "forbid"},
            ),
            Category(
                code="1",
                name="True",
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
            type="integer",
            min_value=0,
            max_value=None,
            decimal_places=None,
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
        cell_regex="[A-Za-z]\\d{11}(\\d|[A-Za-z])",
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
        date="%Y-%m-%dT%H:%M:%S",
        dictionary=None,
        category=None,
        header_regex=None,
        cell_regex=None,
        canbeblank=True,
        model_config={"extra": "forbid"},
    )

    assert column_spec[5].column_spec == Column(
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

    assert column_spec[6].column_spec == Column(
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

    assert column_spec[7].column_spec == Column(
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
