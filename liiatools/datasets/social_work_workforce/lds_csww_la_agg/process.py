from pathlib import Path
import pandas as pd
import logging

log = logging.getLogger(__name__)


def read_file(file):
    """
    Reads the csv file as a pandas DataFrame
    """
    filepath = Path(file)
    csww_df = pd.read_csv(filepath, index_col=None)
    return csww_df


def match_load_file(csww_df, column_names):
    """
    Matches the columns in the DataFrame against one of the 10 SSDA903 file types
    """
    for table_name, expected_columns in column_names.items():
        if set(csww_df.columns) == set(expected_columns):
            return table_name


def merge_la_files(output, csww_df, table_name):
    """
    Looks for existing file of the same type and merges with new file if found
    """
    old_file = Path(output, f"CSWW_{table_name}_merged.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, index_col=None)
        merged_df = pd.concat([csww_df, old_df], axis=0)
    else:
        merged_df = csww_df
    return merged_df


def convert_datetimes(csww_df, dates, table_name):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates[table_name]:
        csww_df[date_field] = pd.to_datetime(csww_df[date_field], format="%Y/%m/%d")
    return csww_df


def deduplicate(csww_df, table_name, sort_order, dedup):
    """
    Sorts and removes duplicate records from merged files following schema
    """
    csww_df = csww_df.sort_values(sort_order[table_name], ascending=False, ignore_index=True)
    csww_df = csww_df.drop_duplicates(subset=dedup[table_name], keep="first")
    return csww_df


def remove_old_data(csww_df, num_of_years, new_year_start_month, as_at_date):
    """
    Removes data older than a specified number of years
    """
    current_year = pd.to_datetime(as_at_date).year
    current_month = pd.to_datetime(as_at_date).month
    if current_month < new_year_start_month:
        earliest_allowed_year = current_year - num_of_years
    else:
        earliest_allowed_year = current_year - num_of_years + 1
    csww_df = csww_df[csww_df["YEAR"] >= earliest_allowed_year]
    return csww_df


def convert_dates(csww_df, dates, table_name):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates[table_name]:
        csww_df[date_field] = pd.to_datetime(
            csww_df[date_field], format="%Y/%m/%d"
        ).dt.date
    return csww_df


def export_la_file(output, table_name, csww_df):
    """
    Writes the output as a csv
    """
    output_path = Path(output, f"CSWW_{table_name}_merged.csv")
    csww_df.to_csv(output_path, index=False)
