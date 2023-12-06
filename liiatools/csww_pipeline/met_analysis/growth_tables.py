import pandas as pd
from datetime import datetime

from liiatools.common.reference import authorities


def _get_year_columns(year, length):
    """
    Create list of years from a given year and for a given length
    :param year: First year to add to list
    :param length: Number of years to be added to list
    :return: List of years length+1 long
    """
    year_columns = []
    for i in range(0, length + 1):
        year_columns.append(str(year + i))
    return year_columns


def growth_tables(public_fs, year):
    """
    Create population growth tables for London LAs for children age 0-18
    """
    with public_fs.open("rb") as f:
        population_growth_table = pd.read_csv(f)

        population_growth_table = population_growth_table[
            population_growth_table["gss_name"].isin(authorities.names)
        ]
        population_growth_table = population_growth_table[
            population_growth_table["age"].isin(range(19))
        ]
        population_growth_table = population_growth_table.groupby(
            "gss_name", as_index=False
        ).agg("sum", numeric_only=True)
        population_growth_table = population_growth_table.rename(
            columns={"gss_name": "LEAName"}
        )

        year_columns = _get_year_columns(year=year, length=10)
        population_growth_table = population_growth_table[["LEAName"] + year_columns]

    return population_growth_table
