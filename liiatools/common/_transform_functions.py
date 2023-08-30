import pandas as pd

from liiatools.common.reference import authorities
from liiatools.datasets.shared_functions.converters import (
    to_nth_of_month,
    to_short_postcode,
)

from .data import ColumnConfig, Metadata


def add_la_suffix(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    return f"{row[column_config.id]}_{metadata['la_code']}"


def add_la_code(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    return metadata["la_code"]


def add_la_name(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    return authorities.get_by_code(metadata["la_code"])


def add_year(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    return metadata["year"]


enrich_functions = {
    "add_la_suffix": add_la_suffix,
    "la_code": add_la_code,
    "la_name": add_la_name,
    "year": add_year,
}


def degrade_to_first_of_month(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    return to_nth_of_month(row[column_config.id], 1)


def degrade_to_short_postcode(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    return to_short_postcode(row[column_config.id])


degrade_functions = {
    "first_of_month": degrade_to_first_of_month,
    "short_postcode": degrade_to_short_postcode,
}
