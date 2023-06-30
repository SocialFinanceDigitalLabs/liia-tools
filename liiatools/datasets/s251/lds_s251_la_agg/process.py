from pathlib import Path
import pandas as pd
import logging

log = logging.getLogger(__name__)


def read_file(file):
    """
    Reads the csv file as a pandas DataFrame
    """
    filepath = Path(file)
    s251_df = pd.read_csv(filepath, index_col=None)
    return s251_df


def merge_la_files(output, s251_df):
    """
    Looks for existing file of the same type and merges with new file if found
    """
    old_file = Path(output, f"S251_merged.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, index_col=None)
        merged_df = pd.concat([s251_df, old_df], axis=0)
    else:
        merged_df = s251_df
    return merged_df


def convert_datetimes(s251_df, dates):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates:
        s251_df[date_field] = pd.to_datetime(s251_df[date_field], format="%Y/%m/%d")
    return s251_df


def deduplicate(s251_df, sort_order, dedup):
    """
    Sorts and removes duplicate records from merged files following schema
    """
    s251_df = s251_df.sort_values(
        sort_order, ascending=False, ignore_index=True
    )
    s251_df = s251_df.drop_duplicates(subset=dedup, keep="first")
    return s251_df


def remove_old_data(s251_df, num_of_years, new_year_start_month, as_at_date):
    """
    Removes data older than a specified number of years as at reference date

    :param s251_df: Dataframe containing csv data
    :param num_of_years: The number of years to go back
    :param new_year_start_month: The month which signifies start of a new year for data retention policy
    :param as_at_date: The reference date against which we are checking the valid range
    :return: Dataframe with older years removed
    """
    current_year = pd.to_datetime(as_at_date).year
    current_month = pd.to_datetime(as_at_date).month

    if current_month < new_year_start_month:
        earliest_allowed_year = current_year - num_of_years
    else:
        earliest_allowed_year = current_year - num_of_years + 1  # roll forward one year

    s251_df = s251_df[s251_df["YEAR"] >= earliest_allowed_year]
    return s251_df


def convert_dates(s251_df, dates):
    """
    Ensures that all date fields have been parsed as dates
    """
    for date_field in dates:
        s251_df[date_field] = pd.to_datetime(
            s251_df[date_field], format="%Y/%m/%d"
        ).dt.date
    return s251_df


def export_la_file(output, s251_df):
    """
    Writes the output as a csv
    """
    output_path = Path(output, f"S251_merged.csv")
    s251_df.to_csv(output_path, index=False)
