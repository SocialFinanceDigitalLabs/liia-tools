from sfdata_stream_parser.events import Cell

from liiatools.annex_a_pipeline.stream_filters import convert_column_header_to_match
from liiatools.common.spec.__data_schema import DataSchema, Column


def test_convert_column_header_to_match():
    schema = DataSchema(
        column_map={
            "List 1": {
                "Child Unique ID": Column(header_regex=["/.*child.*id.*/i"]),
                "Gender": Column(),
            }
        }
    )
    stream = [
        Cell(table_name="List 1", header="Child Unique ID"),
        Cell(table_name="List 1", header="Child ID"),
        Cell(table_name="List 1", header="Gender"),
    ]
    stream = list(convert_column_header_to_match(stream, schema=schema))

    assert stream[0].header == "Child Unique ID"
    assert stream[1].header == "Child Unique ID"
    assert stream[2].header == "Gender"
