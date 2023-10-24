from sfdata_stream_parser.filters import generic

from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import DataContainer, FileLocator, ProcessResult
from liiatools.datasets.shared_functions import common as common_functions
from liiatools.common.spec.__data_schema import DataSchema
from liiatools.common.stream_pipeline import to_dataframe

from . import stream_filters


def task_cleanfile(src_file: FileLocator, schema: DataSchema) -> ProcessResult:
    # Open & Parse file
    stream = stream_functions.tablib_parse(src_file)

    # Configure stream
    stream = stream_functions.add_table_name(stream, schema=schema)
    stream = common_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_filters.convert_column_header_to_match(stream, schema=schema)
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
