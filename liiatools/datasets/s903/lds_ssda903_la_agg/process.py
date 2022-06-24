from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
import numpy as np
import os

log = logging.getLogger(__name__)
start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"


def read_file(file):
    """
    Reads the csv file as a pandas DataFrame
    """
    filepath = Path(file)
    df = pd.read_csv(filepath, index_col=None, dtype=object)
    return df


def match_load_file(s903_df, column_names):
    """
    Matches the columns in the DataFrame against one of the 10 SSDA903 file types
    """
    for table_name, expected_columns in column_names.items():
        if set(s903_df.columns) == set(expected_columns):
            return table_name


def merge_la_files(output, s903_df, table_name):
    '''
    Looks for existing file of the same type and merges with new file if found
    '''
    old_file = Path(output, f"SSDA903_{table_name}_merged.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, index_col=None, dtype=object)
        merged_df = pd.concat([s903_df, old_df], axis=0)
    else:
        merged_df = s903_df
    return merged_df


def convert_datetimes(s903_df, dates, table_name):
    '''
    Ensures that all date fields have been parsed as dates
    '''
    for date_field in dates[table_name]:
        s903_df[date_field] = pd.to_datetime(s903_df[date_field], format="%Y/%m/%d")
    return s903_df


def deduplicate(df, table_name, sort_order, dedup):
    '''
    Sorts and removes duplicate records from merged files following schema
    '''
    if table_name in sort_order.keys():
        df = df.sort_values(sort_order[table_name], ascending = False)
    if table_name in dedup.keys():
        df = df.drop_duplicates(subset=dedup[table_name], keep = 'first')
    return df


def remove_old_data(s903_df, years):
    '''
    Removes data older than a specified number of years
    '''
    year = pd.to_datetime('today').year
    month = pd.to_datetime('today').month
    if month <= 6:
        year = year - 1
    s903_df = s903_df[s903_df['YEAR'] >= year - years]
    return s903_df


def log_missing_years(s903_df, table_name, la_log_dir):
    """
    Output a log of the missing years for merged dataframes
    """
    expected_years = pd.Series(np.arange(s903_df["YEAR"].min(), s903_df["YEAR"].max()+1))
    actual_years = s903_df["YEAR"].unique()
    missing_years = expected_years[~expected_years.isin(actual_years)]
    clean_missing_years = str(missing_years.values)[1:-1]  # Remove brackets from missing_years

    if clean_missing_years:
        with open(f"{os.path.join(la_log_dir, table_name)}_error_log_{start_time}.txt", 'a') as f:
            f.write(f"{table_name}_{start_time}")
            f.write("\n")
            f.write(f"Years missing from dataset: {clean_missing_years}")
            f.write("\n")


def convert_dates(s903_df, dates, table_name):
    '''
    Ensures that all date fields have been parsed as dates
    '''
    for date_field in dates[table_name]:
        s903_df[date_field] = pd.to_datetime(s903_df[date_field], format="%Y/%m/%d").dt.date
    return s903_df


def export_la_file(output, table_name, s903_df):
    output_path = Path(output, f"SSDA903_{table_name}_merged.csv")
    s903_df.to_csv(output_path, index=False)