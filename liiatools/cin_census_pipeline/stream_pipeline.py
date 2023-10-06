from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pandas as pd
from sfdata_stream_parser import events
from sfdata_stream_parser.filters import generic
from sfdata_stream_parser.parser.xml import parse
from xmlschema import XMLSchema

from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import DataContainer, FileLocator, ProcessResult
from liiatools.datasets.cin_census.lds_cin_clean import cin_record
from liiatools.datasets.cin_census.lds_cin_clean.parse import dom_parse
from liiatools.datasets.shared_functions import common as common_functions

from . import stream_filters as filters


def task_cleanfile(src_file: FileLocator, schema: XMLSchema) -> ProcessResult:
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
