from pathlib import Path
import pandas as pd
import logging

log = logging.getLogger(__name__)


def read_file(file: str, dates: list):
    """
    Reads the csv file as a pandas DataFrame

    :param file: Location of file to read
    :param dates: Headers that should be dates
    :return: Dataframe of the file with correctly formatted dates
    """
    filepath = Path(file)
    csww_df = pd.read_csv(filepath, index_col=None, parse_dates=dates, infer_datetime_format=True)
    return csww_df
