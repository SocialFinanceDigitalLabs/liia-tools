import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
from enum import Enum


class DataType(Enum):
    EMPTY_COLUMN = "empty_column"
    MISSING_COLUMN = "missing_column"


def _save_year_error(input: str, la_log_dir: str, data_type: DataType):
    """
    Save errors to a text file in the LA log directory

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :param data_type: The type of year error e.g. missing column, empty correct column
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
                f"Could not process '{filename}' because placement end date column was not found which is used to "
                f"identify the year of return"
            )
        if data_type == DataType.EMPTY_COLUMN:
            f.write(
                f"Could not process '{filename}' because placement end date column was empty which is used to "
                f"identify the year of return"
            )


def find_year_of_return(input: str, la_log_dir: str):
    """
    Checks the minimum placement end date years to find year and quarter of return

    :param input: Path to file that we need to find the year of return from
    :param la_log_dir: Path to the local authority's log folder
    :return: A year and quarter of return
    """
    infile = Path(input)
    try:
        data = pd.read_csv(infile, usecols=["Placement end date"])
        data["Placement end date"] = pd.to_datetime(
            data["Placement end date"], format="%d/%m/%Y", errors="coerce"
        )
        year = data["Placement end date"].min().year
        quarter = f'Q{(data["Placement end date"].min().month - 1) // 3}'
        if year is np.nan:
            _save_year_error(input, la_log_dir, DataType.EMPTY_COLUMN)
            return None, None
        else:
            quarter = "Q4" if quarter == "Q0" else quarter
            return year, quarter
    except ValueError as e:
        if "columns expected but not found: ['Placement end date']" in str(e):
            _save_year_error(input, la_log_dir, DataType.MISSING_COLUMN)
            return None, None
