import tablib
from xmlschema import XMLSchema

from liiatools.common.data import FileLocator, ProcessResult
from liiatools.cin_census_pipeline import stream_record
from liiatools.cin_census_pipeline.stream_parse import dom_parse

from . import stream_filters as filters

# TODO: Should return a ProcessResult with a dataframe, not tablib


def task_cleanfile(src_file: FileLocator, schema: XMLSchema) -> tablib.Dataset:
    with src_file.open("rb") as f:
        stream = dom_parse(f)

        stream = filters.strip_text(stream)
        stream = filters.add_context(stream)
        stream = filters.add_schema(stream, schema=schema)

        stream = filters.convert_true_false(stream)

        stream = filters.validate_elements(stream)
        stream = filters.remove_invalid(stream)

        stream = stream_record.message_collector(stream)
        data = stream_record.export_table(stream)

    return data
