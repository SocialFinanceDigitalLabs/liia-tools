from pathlib import Path
from xmlschema import XMLSchema
import pandas as pd

from sfdata_stream_parser.filters import generic

from liiatools.common.data import FileLocator, ProcessResult, DataContainer
from liiatools.common import stream_filters as stream_functions
from liiatools.common.stream_parse import dom_parse
from liiatools.common.stream_pipeline import to_dataframe_xml

from liiatools.cin_census_pipeline import stream_record

from . import stream_filters as filters


def task_cleanfile(
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

        # dataset = DataContainer({k: pd.DataFrame(v) for k, v in dataset.items()})
        dataset = DataContainer(
            {k: to_dataframe_xml(v, schema_path) for k, v in dataset.items()}
        )

    return ProcessResult(data=dataset, errors=errors)
