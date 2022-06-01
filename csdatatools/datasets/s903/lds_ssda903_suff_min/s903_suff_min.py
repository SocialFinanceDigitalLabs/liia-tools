from datetime import datetime
import os
from pathlib import Path
import pandas as pd
import logging

from csdatatools.datasets.s903.lds_ssda903_suff_min.s903_suff_min_config import column_names
from csdatatools.datasets.s903.lds_ssda903_suff_min.s903_suff_min_config import minimise

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

log = logging.getLogger(__name__)


def suff_read_file(input):
    """
    Reads the csv file as a pandas DataFrame
    """
    try:
        filepath = Path(input)
        df = pd.read_csv(filepath)
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def suff_match_load_file(df):
    """
    Matches the columns in the DataFrame against one of the 8 SSDA903 file types
    """
    try:
        for table_name, expected_columns in column_names.items():
            if set(df.columns) == set(expected_columns):
                return table_name
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def data_min(df, table_name):
    """
    Performs additional data minimisation for pan-London sufficiency analysis in line with project specification
    """
    try:
        df = df.drop(columns=minimise[table_name], axis=1)
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def export_suff_file(output, table_name, df):
    """
    Writes the minimised output as a csv
    """
    try:
        output_path = Path(output, f"pan_London_suff_SSDA903_{table_name}.csv")
        df.to_csv(output_path, index=False)
    except Exception as ex:
        log.exception(f"Error occurred in {input}")
