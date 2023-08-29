from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pandas as pd
import tablib
from sfdata_stream_parser import collectors, events
from sfdata_stream_parser.filters import generic

from liiatools.datasets.shared_functions import common as common_functions
from liiatools.datasets.shared_functions import filesystem
from liiatools.datasets.shared_functions import stream as stream_functions
from liiatools.datasets.shared_functions.converters import (
    to_nth_of_month,
    to_short_postcode,
)

from .lds_ssda903_clean import filters
from .spec import Column, ColumnConfig, DataSchema, PipelineConfig, TableConfig

DataContainer = Dict[str, pd.DataFrame]


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
    stream = filters.add_table_name(stream, schema=schema)
    stream = common_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = filters.match_config_to_cell(stream, schema=schema)

    # Clean stream
    stream = filters.clean_dates(stream)
    stream = filters.clean_categories(stream)
    stream = filters.clean_integers(stream)
    stream = filters.clean_postcodes(stream)

    # Create dataset
    stream = filters.collect_cell_values_for_row(stream)
    dataset_holder, stream = filters.collect_tables(stream)

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value

    dataset = {k: pd.DataFrame(v) for k, v in dataset.items()}

    return CleanFileResult(data=dataset)


def add_la_suffix(row: pd.Series, column_config: ColumnConfig) -> str:
    return f"{row[column_config.id]}_LA_CODE"


def add_la_code(row: pd.Series, column_config: ColumnConfig) -> str:
    return "LA_CODE"


def add_year(row: pd.Series, column_config: ColumnConfig) -> str:
    return "YEAR"


_enrich_functions = {
    "add_la_suffix": add_la_suffix,
    "la_code": add_la_code,
    "year": add_year,
}


def _enrich(data: pd.DataFrame, table_config: TableConfig):
    for column_config in table_config.columns:
        if column_config.enrich:
            data[column_config.id] = data.apply(
                lambda row: _enrich_functions[column_config.enrich](row, column_config),
                axis=1,
            )


def task_enrich(data: DataContainer, config: PipelineConfig) -> DataContainer:
    # Create a copy of the data so we don't mutate the original
    data = {k: v.copy() for k, v in data.items()}

    # Loop over known tables
    for table_config in config.table_list:
        if table_config.id in data:
            _enrich(data[table_config.id], table_config)
    return data


def first_of_month(row: pd.Series, column_config: ColumnConfig) -> str:
    return to_nth_of_month(row[column_config.id], 1)


def short_postcode(row: pd.Series, column_config: ColumnConfig) -> str:
    return to_short_postcode(row[column_config.id])


_degrade_functions = {
    "first_of_month": first_of_month,
    "short_postcode": short_postcode,
}


def _degrade(data: pd.DataFrame, table_config: TableConfig):
    for column_config in table_config.columns:
        if column_config.degrade:
            data[column_config.id] = data.apply(
                lambda row: _degrade_functions[column_config.degrade](
                    row, column_config
                ),
                axis=1,
            )


def task_degrade(data: DataContainer, config: PipelineConfig) -> DataContainer:
    # Create a copy of the data so we don't mutate the original
    data = {k: v.copy() for k, v in data.items()}

    # Loop over known tables
    for table_config in config.table_list:
        if table_config.id in data:
            _degrade(data[table_config.id], table_config)
    return data
