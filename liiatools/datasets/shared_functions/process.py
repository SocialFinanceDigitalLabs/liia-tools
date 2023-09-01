from pathlib import Path
import pandas as pd
import logging

log = logging.getLogger(__name__)


def read_file(file):
    """
    Reads the csv file as a pandas DataFrame
    """
    filepath = Path(file)
    df = pd.read_csv(filepath, index_col=None)
    return df


def match_load_file(df, column_names):
    """
    Matches the columns in the DataFrame against one of the given file types
    """
    for table_name, expected_columns in column_names.items():
        if set(df.columns) == set(expected_columns):
            return table_name


def merge_la_files(output, df, table_name, filename):
    """
    Looks for existing file of the same type and merges with new file if found
    """
    old_file = Path(output, f"{filename}_{table_name}_merged.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, index_col=None)
        merged_df = pd.concat([df, old_df], axis=0)
    else:
        merged_df = df
    return merged_df


def convert_datetimes(df, dates, table_name):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates[table_name]:
        df[date_field] = pd.to_datetime(df[date_field], format="%Y/%m/%d")
    return df


def deduplicate(df, table_name, sort_order, dedup):
    """
    Sorts and removes duplicate records from merged files following schema
    """
    df = df.sort_values(sort_order[table_name], ascending=False, ignore_index=True)
    df = df.drop_duplicates(subset=dedup[table_name], keep="first")
    return df


def remove_old_data(df, num_of_years, new_year_start_month, as_at_date, year_column):
    """
    Removes data older than a specified number of years as at reference date

    :param df: Dataframe containing csv data
    :param num_of_years: The number of years to go back
    :param new_year_start_month: The month which signifies start of a new year for data retention policy
    :param as_at_date: The reference date against which we are checking the valid range
    :param year_column: Column name that contains year data
    :return: Dataframe with older years removed
    """
    current_year = pd.to_datetime(as_at_date).year
    current_month = pd.to_datetime(as_at_date).month

    if current_month < new_year_start_month:
        earliest_allowed_year = current_year - num_of_years
    else:
        earliest_allowed_year = current_year - num_of_years + 1  # roll forward one year

    df = df[df[year_column] >= earliest_allowed_year]
    return df


def convert_dates(df, dates, table_name):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates[table_name]:
        df[date_field] = pd.to_datetime(df[date_field], format="%Y/%m/%d").dt.date
    return df


def export_la_file(output, table_name, df, filename):
    """
    Writes the output as a csv
    """
    output_path = Path(output, f"{filename}_{table_name}_merged.csv")
    df.to_csv(output_path, index=False)


def _merge_dfs(df, old_df, la_name):
    """
    Deletes existing data for new LA from pan file
    Merges new LA data to pan file
    """
    old_df = old_df.drop(old_df[old_df["LA"] == la_name].index)
    df = pd.concat([df, old_df], axis=0, ignore_index=True)
    return df


def merge_agg_files(output, table_name, df, la_name, filename):
    """
    Checks if pan file exists
    Passes old and new file to function to be merged
    """
    output_file = Path(output, f"pan_London_{filename}_{table_name}.csv")
    if output_file.is_file():
        old_df = pd.read_csv(output_file, index_col=None)
        df = _merge_dfs(df, old_df, la_name)
    return df


def export_pan_file(output, table_name, df, filename):
    """
    Writes file to output directory
    """
    output_path = Path(output, f"pan_London_{filename}_{table_name}.csv")
    df.to_csv(output_path, index=False)
