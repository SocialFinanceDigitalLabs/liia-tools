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


def drop_empty_rows(infile: str, outfile: str):
    """
    Csv drop empty rows at top of file, save output

    :param infile: Path to file that needs to be cleaned
    :param outfile: Path where cleaned file will be saved
    """
    infile = Path(infile)
    data = pd.read_csv(infile, skip_blank_lines=True)
    logging.info(f"removing blank rows in {infile.stem}")
    data = data.dropna(how="all")
    data.to_csv(outfile, header=True, encoding="utf-8", index=False)
    return data
