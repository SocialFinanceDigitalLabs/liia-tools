from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
import numpy as np
import os

from csdatatools.datasets.s903.lds_ssda903_la_agg.s903_la_agg_config import column_names
from csdatatools.datasets.s903.lds_ssda903_la_agg.s903_la_agg_config import dates
from csdatatools.datasets.s903.lds_ssda903_la_agg.s903_la_agg_config import sort_order
from csdatatools.datasets.s903.lds_ssda903_la_agg.s903_la_agg_config import dedup

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

log = logging.getLogger(__name__)


def la_read_file(file):
    """
    Reads the csv file as a pandas DataFrame
    """
    try:
        filepath = Path(file)
        df = pd.read_csv(filepath)
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def la_match_load_file(df):
    """
    Matches the columns in the DataFrame against one of the 10 SSDA903 file types
    """
    try:
        for table_name, expected_columns in column_names.items():
            if set(df.columns) == set(expected_columns):
                return table_name
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def merge_la_files(output, table_name, new_df):
    try:
        file_list = Path(output).glob('*.csv')
        old_dict = {}
        for file in file_list:
            old_df = pd.read_csv(file)
            old_table = la_match_load_file(old_df)
            old_dict[old_table] = old_df
        if table_name in old_dict.keys():
            merged_df = pd.concat([new_df, old_dict[table_name]], axis=0)
        else:
            merged_df = new_df
        return merged_df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def convert_dates(df, table_name):
    try:
        for date_field in dates[table_name]:
            df[date_field] = pd.to_datetime(df[date_field], format="%Y/%m/%d").dt.date
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def deduplicate(df, table_name):
    try:
        if table_name in sort_order.keys():
            df = df.sort_values(sort_order[table_name], ascending = False)
        if table_name in dedup.keys():
            df = df.drop_duplicates(subset=dedup[table_name], keep = 'first')
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def remove_old_data(df):
    try:
        year = pd.to_datetime('today').year
        month = pd.to_datetime('today').month
        if month <= 6:
            year = year - 1
        df = df[df['YEAR'] >= year - 5]
        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def log_missing_years(df, table_name, la_log_dir):
    """
    Output a log of the missing years for merged dataframes
    """
    try:
        expected_years = pd.Series(np.arange(df["YEAR"].min(), df["YEAR"].max()+1))
        actual_years = df["YEAR"].unique()
        missing_years = expected_years[~expected_years.isin(actual_years)]
        clean_missing_years = str(missing_years.values)[1:-1]  # Remove brackets from missing_years

        if clean_missing_years:
            with open(f"{os.path.join(la_log_dir, table_name)}_error_log_{start_time}.txt", 'a') as f:
                f.write(f"{table_name}_{start_time}")
                f.write("\n")
                f.write(f"Years missing from dataset: {clean_missing_years}")
                f.write("\n")

        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def export_la_file(output, table_name, df):
    try:
        output_path = Path(output, f"SSDA903_{table_name}_merged.csv")
        df.to_csv(output_path, index=False)
    except Exception as ex:
        log.exception(f"Error occurred in {input}")
