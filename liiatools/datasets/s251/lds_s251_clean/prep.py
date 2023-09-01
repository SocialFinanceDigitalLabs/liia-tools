import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
from enum import Enum


class DataType(Enum):
    EMPTY_COLUMN = "empty_column"
    MISSING_COLUMN = "missing_column"
    OLD_DATA = "old_data"


def _save_year_error(
    input: str, la_log_dir: str, data_type: DataType, year: int = None
):
    """
    Save errors to a text file in the LA log directory

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :param data_type: The type of year error e.g. missing column, empty correct column, data too old
    :param year: Year of the data
    :return: Text file containing the error information
    """

    filename = Path(input).resolve().stem
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"
    with open(
        f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
        "a",
    ) as f:
        if data_type == DataType.MISSING_COLUMN:
            f.write(
                f"Could not process '{filename}' because end date column was not found which is used to "
                f"identify the year of return"
            )
        if data_type == DataType.EMPTY_COLUMN:
            f.write(
                f"Could not process '{filename}' because end date column was empty which is used to identify "
                f"the year of return"
            )
        if data_type == DataType.OLD_DATA:
            f.write(
                f"Could not process '{filename}' because this file is from the year {year} and we are only accepting "
                f"data from 2023 onwards"
            )


def _calculate_year_quarter(input: pd.DataFrame, date_column: str):
    """
    Calculate the minimum year and quarter of a given dataframe based on data_column

    :param input: Dataframe that we need to find the year and quarter of return
    :param date_column: Column which contains date information required to find year of return
    :return: Minimum year and quarter from the given date_column
    """
    input[date_column] = pd.to_datetime(
        input[date_column], format="%d/%m/%Y", errors="coerce"
    )
    year = input[date_column].min().year
    quarter = f"Q{(input[date_column].min().month - 1) // 3}"
    return year, quarter


def find_year_of_return(
    input: str, la_log_dir: str, retention_period: int, reference_year: int
):
    """
    Checks the minimum placement end date years to find year and quarter of return

    :param input: Path to file that we need to find the year of return from
    :param la_log_dir: Path to the local authority's log folder
    :param retention_period: Number of years in the retention period
    :param reference_year: The reference date against which we are checking the valid range
    :return: A year and quarter of return
    """
    infile = Path(input)
    data = pd.read_csv(
        infile, usecols=lambda x: x in ["Placement end date", "End date"]
    )

    if "Placement end date" in data:
        year, quarter = _calculate_year_quarter(data, "Placement end date")
    elif "End date" in data:
        year, quarter = _calculate_year_quarter(data, "End date")
    else:
        _save_year_error(input, la_log_dir, DataType.MISSING_COLUMN)
        return None, None

    if year is np.nan:
        _save_year_error(input, la_log_dir, DataType.EMPTY_COLUMN)
        return None, None
    else:
        quarter = "Q4" if quarter == "Q0" else quarter
        year = year + 1 if quarter == "Q1" or quarter == "Q2" else year
        if year in range(reference_year - retention_period, 2023):
            _save_year_error(input, la_log_dir, DataType.OLD_DATA, year=year)
            return None, None
        else:
            return year, quarter
