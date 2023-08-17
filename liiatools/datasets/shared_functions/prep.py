import pandas.errors
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime


def check_blank_file(input: str, la_log_dir: str):
    """
    Check csv file is not empty

    :param input: Path to file that needs to be checked
    :param la_log_dir: Location to save the error log
    :return: If csv is empty stop process and write log to local authority, else continue
    """
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"
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
        return "error"
    except UnicodeDecodeError:
        with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
        ) as f:
            f.write(
                f"File: '{filename}{extension}' was found to be saved in the wrong format. The correct format"
                f" is CSV UTF-8. To fix this open the file in Excel, click 'Save As' and select 'CSV UTF-8"
                f" (Comma delimited) (*csv)'"
            )
        return "error"


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
