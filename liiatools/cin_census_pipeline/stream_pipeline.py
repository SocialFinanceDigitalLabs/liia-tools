from pathlib import Path
from xmlschema import XMLSchema

from sfdata_stream_parser.filters import generic

from liiatools.common.data import FileLocator, ProcessResult, DataContainer
from liiatools.common import stream_filters as stream_functions
from liiatools.common.spec.__data_schema import DataSchema
from liiatools.common.stream_parse import dom_parse
from liiatools.common.stream_pipeline import to_dataframe_xml, to_dataframe

from liiatools.cin_census_pipeline import stream_record

from . import stream_filters as filters


def task_xml_cleanfile(
    src_file: FileLocator, schema: XMLSchema, schema_path: Path
) -> ProcessResult:
    """
    Clean input cin census xml files according to schema and output clean data and errors
    :param src_file: The pointer to a file in a virtual filesystem
    :param schema: The data schema
    :param schema_path: Path to the data schema
    :return: A class containing a DataContainer and ErrorContainer
    """
    with src_file.open("rb") as f:
        # Open & Parse file
        stream = dom_parse(f, filename=src_file.name)

        # Configure stream
        stream = stream_functions.strip_text(stream)
        stream = stream_functions.add_context(stream)
        stream = stream_functions.add_schema(stream, schema=schema)
        stream = filters.add_column_spec(stream, schema_path=schema_path)

        # Clean stream
        stream = stream_functions.log_blanks(stream)
        stream = stream_functions.conform_cell_types(stream)
        stream = stream_functions.validate_elements(stream)

        # Create dataset
        error_holder, stream = stream_functions.collect_errors(stream)
        stream = stream_record.message_collector(stream)
        dataset_holder, stream = stream_record.export_table(stream)

        # Consume stream so we know it's been processed
        generic.consume(stream)

        dataset = dataset_holder.value
        errors = error_holder.value

        dataset = DataContainer(
            {k: to_dataframe_xml(v, schema_path) for k, v in dataset.items()}
        )

    return ProcessResult(data=dataset, errors=errors)


def task_csv_cleanfile(src_file: FileLocator, schema: DataSchema) -> ProcessResult:
    # Open & Parse file
    stream = stream_functions.tablib_parse(src_file)

    # Configure stream
    stream = stream_functions.add_table_name(stream, schema=schema)
    stream = stream_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_functions.match_config_to_cell(stream, schema=schema)

    # Clean stream
    stream = stream_functions.log_blanks(stream)
    stream = stream_functions.conform_cell_types(stream)

    # Create dataset
    stream = stream_functions.collect_cell_values_for_row(stream)
    dataset_holder, stream = stream_functions.collect_tables(stream)
    error_holder, stream = stream_functions.collect_errors(stream)

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value
    errors = error_holder.value

    dataset = DataContainer(
        {k: to_dataframe(v, schema.table[k]) for k, v in dataset.items()}
    )

    return ProcessResult(data=dataset, errors=errors)
