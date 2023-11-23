import pandas as pd
from xmlschema import XMLSchema
from pathlib import Path

from sfdata_stream_parser.filters import generic

from liiatools.common.data import FileLocator, ProcessResult, DataContainer
from liiatools.common import stream_filters as stream_functions
from liiatools.csww_pipeline import stream_record
from liiatools.csww_pipeline.stream_parse import dom_parse

from . import stream_filters as filters


def task_cleanfile(src_file: FileLocator, schema: XMLSchema, schema_path: Path) -> ProcessResult:
    with src_file.open("rb") as f:
        # Open & Parse file
        stream = dom_parse(f, filename=src_file.name)

        # Configure stream
        stream = filters.strip_text(stream)
        stream = filters.add_context(stream)
        stream = filters.add_schema(stream, schema=schema)
        stream = filters.add_column_spec(stream, schema_path=schema_path)

        # Clean stream
        stream = stream_functions.log_blanks(stream)
        stream = stream_functions.conform_cell_types(stream)
        stream = filters.validate_elements(stream)

        # Create dataset
        error_holder, stream = stream_functions.collect_errors(stream)
        stream = stream_record.message_collector(stream)
        dataset_holder, stream = stream_record.export_table(stream)

        # Consume stream so we know it's been processed
        generic.consume(stream)

        dataset = dataset_holder.value
        # errors = error_holder.value

        dataset = DataContainer(
            {k: pd.DataFrame(v) for k, v in dataset.items()}
        )

    return ProcessResult(data=dataset, errors=None)
