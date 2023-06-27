import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import pandas.errors


def check_blank_file(input: str, la_log_dir: str):
    """
    Check csv file is not empty

    :param input: Path to file that needs to be checked
    :param la_log_dir: Location to save the error log
    :return: If csv is empty stop process and write log to local authority, else continue
    """
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    input = Path(input)
    filename = input.resolve().stem
    extension = Path(input).suffix
    try:
        pd.read_csv(input)
        pass
    except pandas.errors.EmptyDataError:
        with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
        ) as f:
            f.write(f"File: '{filename}{extension}' was found to be completely empty")
        return "empty"


def drop_empty_rows(input: str, output: str):
    """
    Csv drop empty rows at top of file, save output

    :param input: Path to file that needs to be cleaned
    :param output: Path where cleaned file will be saved
    """
    input = Path(input)
    data = pd.read_csv(input, skip_blank_lines=True)
    logging.info(f"removing blank rows in {input.stem}")
    data = data.dropna(how="all")
    data.to_csv(output, header=True, encoding="utf-8", index=False)
    return data


def _save_year_error(input: str, la_log_dir: str):
    """
    Save errors to a text file in the LA log directory

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :return: Text file containing the error information
    """

    filename = Path(input).resolve().stem
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
    ) as f:
        f.write(
            f"Could not process '{filename}' because placement end date column was not found which is used to identify"
            f"the year of return"
        )


def find_year_of_return(input: str, la_log_dir: str):
    """
    Checks the minimum placement end date years to find year of return

    :param input: Path to file that we need to find the year of return from
    :param la_log_dir: Path to the local authority's log folder
    :return: A year of return
    """
    infile = Path(input)
    try:
        data = pd.read_csv(infile, usecols=["Placement end date"])
        data["Placement end date"] = pd.to_datetime(data["Placement end date"], format="%d/%m/%Y")
        year = data["Placement end date"].min().year
        return year
    except ValueError:
        _save_year_error(input, la_log_dir)
        return
