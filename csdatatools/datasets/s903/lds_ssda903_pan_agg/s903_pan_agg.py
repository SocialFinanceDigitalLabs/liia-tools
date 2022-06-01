import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

from csdatatools.datasets.s903.lds_ssda903_pan_agg.s903_pan_agg_config import column_names

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

log = logging.getLogger(__name__)


def pan_read_file(input):
    """
    Reads the csv file as a pandas DataFrame
    """
    try:
        filepath = Path(input)
        df = pd.read_csv(filepath)
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def pan_match_load_file(df):
    """
    Matches the columns in the DataFrame against one of the 10 SSDA903 file types
    """
    try:
        for table_name, expected_columns in column_names.items():
            if set(df.columns) == set(expected_columns):
                return table_name
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def merge_agg_files(output, table_name, la_name, new_df):
    try:
        file_list = Path(output).glob('*.csv')
        old_dict = {}
        for file in file_list:
            old_df = pd.read_csv(file)
            old_table = pan_match_load_file(old_df)
            old_dict[old_table] = old_df
        if table_name in old_dict.keys():
            rel_df = old_dict[table_name]
            rel_df = rel_df.drop(rel_df[rel_df['LA'] == la_name].index)
            merged_df = pd.concat([new_df, rel_df], axis=0)
        else:
            merged_df = new_df
        return merged_df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def export_pan_file(output, table_name, df):
    try:
        output_path = Path(output, f"pan_London_SSDA903_{table_name}.csv")
        df.to_csv(output_path, index=False)
    except Exception as ex:
        log.exception(f"Error occurred in {input}")
