from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pandas as pd
from sfdata_stream_parser.filters import generic

from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import DataContainer, FileLocator
from liiatools.datasets.shared_functions import common as common_functions

from . import stream_filters
from .spec import DataSchema


@dataclass
class CleanFileResult:
    data: DataContainer
    errors: List[Dict[str, Any]]


def task_cleanfile(src_file: FileLocator, schema: DataSchema) -> CleanFileResult:
    # Open & Parse file
    stream = stream_functions.tablib_parse(src_file)

    # Configure stream
    stream = stream_filters.add_table_name(stream, schema=schema)
    stream = common_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_filters.match_config_to_cell(stream, schema=schema)

    # Clean stream
    stream = stream_filters.log_blanks(stream)
    stream = stream_filters.conform_cell_types(stream)

    # Create dataset
    stream = stream_filters.collect_cell_values_for_row(stream)
    dataset_holder, stream = stream_filters.collect_tables(stream)
    errror_holder, stream = stream_filters.collect_errors(stream)

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value

    dataset = {k: pd.DataFrame(v) for k, v in dataset.items()}

    return CleanFileResult(data=dataset, errors=errror_holder.value)
