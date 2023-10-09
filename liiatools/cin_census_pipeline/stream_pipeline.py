import tablib
from xmlschema import XMLSchema

from liiatools.common.data import FileLocator, ProcessResult
from liiatools.datasets.cin_census.lds_cin_clean import cin_record
from liiatools.datasets.cin_census.lds_cin_clean.parse import dom_parse

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

        stream = cin_record.message_collector(stream)
        data = cin_record.export_table(stream)

    return data
