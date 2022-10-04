from pathlib import Path
import pandas as pd
import logging

log = logging.getLogger(__name__)


def read_file(file):
    """
    Reads the csv file as a pandas DataFrame
    """
    filepath = Path(file)
    s903_df = pd.read_csv(filepath, index_col=None)
    return s903_df


def match_load_file(s903_df, column_names):
    """
    Matches the columns in the DataFrame against one of the 10 SSDA903 file types
    """
    for table_name, expected_columns in column_names.items():
        if set(s903_df.columns) == set(expected_columns):
            return table_name


def merge_la_files(output, s903_df, table_name):
    """
    Looks for existing file of the same type and merges with new file if found
    """
    old_file = Path(output, f"SSDA903_{table_name}_merged.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, index_col=None)
        merged_df = pd.concat([s903_df, old_df], axis=0)
    else:
        merged_df = s903_df
    return merged_df


def convert_datetimes(s903_df, dates, table_name):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates[table_name]:
        s903_df[date_field] = pd.to_datetime(s903_df[date_field], format="%Y/%m/%d")
    return s903_df


def deduplicate(s903_df, table_name, sort_order, dedup):
    """
    Sorts and removes duplicate records from merged files following schema
    """
    s903_df = s903_df.sort_values(
        sort_order[table_name], ascending=False, ignore_index=True
    )
    s903_df = s903_df.drop_duplicates(subset=dedup[table_name], keep="first")
    return s903_df


def remove_old_data(s903_df, years):
    """
    Removes data older than a specified number of years
    """
    year = pd.to_datetime("today").year
    month = pd.to_datetime("today").month
    if month <= 6:
        year = year - 1
    s903_df = s903_df[s903_df["YEAR"] >= year - years]
    return s903_df


def convert_dates(s903_df, dates, table_name):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates[table_name]:
        s903_df[date_field] = pd.to_datetime(
            s903_df[date_field], format="%Y/%m/%d"
        ).dt.date
    return s903_df


def export_la_file(output, table_name, s903_df):
    """
    Writes the output as a csv
    """
    output_path = Path(output, f"SSDA903_{table_name}_merged.csv")
    s903_df.to_csv(output_path, index=False)
