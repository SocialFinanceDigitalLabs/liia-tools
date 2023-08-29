from typing import Union

import tablib

from liiatools.datasets.s903.lds_ssda903_clean import filters
from liiatools.datasets.shared_functions import common as common_functions
from liiatools.datasets.shared_functions import filesystem
from liiatools.datasets.shared_functions import stream as stream_functions
from liiatools.spec.s903 import DataSchema


class CleanFileResult:
    pass


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
    stream = filters.add_table_name(stream, schema=schema)
    stream = common_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = filters.match_config_to_cell(stream, schema=schema)

    # Clean stream
    stream = filters.clean_dates(stream)
    stream = filters.clean_categories(stream)
    stream = filters.clean_integers(stream)
    stream = filters.clean_postcodes(stream)

    # TODO: Collect errors and data
