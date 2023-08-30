from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pandas as pd
import tablib
from sfdata_stream_parser.filters import generic

from liiatools.common.data import DataContainer
from liiatools.datasets.shared_functions import common as common_functions
from liiatools.datasets.shared_functions import filesystem
from liiatools.datasets.shared_functions import stream as stream_functions

from . import stream_filters
from .spec import DataSchema


@dataclass
class CleanFileResult:
    data: DataContainer


def task_cleanfile(
    src_file: Union[tablib.Dataset, str], schema: DataSchema
) -> CleanFileResult:
    # TODO: We can improve this to make it easier - but let's see how the other datasets come along first
    if isinstance(src_file, tablib.Dataset):
        dataset = src_file
    else:
        with filesystem.open_file(src_file) as f:
            dataset = tablib.import_set(f, format="csv")

    # Open & Parse file
    stream = stream_functions.tablib_to_stream(dataset)

    # Configure stream
    stream = stream_filters.add_table_name(stream, schema=schema)
    stream = common_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_filters.match_config_to_cell(stream, schema=schema)

    # Clean stream
    stream = stream_filters.clean_dates(stream)
    stream = stream_filters.clean_categories(stream)
    stream = stream_filters.clean_integers(stream)
    stream = stream_filters.clean_postcodes(stream)

    # Create dataset
    stream = stream_filters.collect_cell_values_for_row(stream)
    dataset_holder, stream = stream_filters.collect_tables(stream)

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value

    dataset = {k: pd.DataFrame(v) for k, v in dataset.items()}

    return CleanFileResult(data=dataset)
