from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np
import logging

log = logging.getLogger(__name__)


def read_file(input, dates):
    """
    Reads the csv file as a pandas DataFrame
    """
    flatfile = pd.read_csv(input, parse_dates=dates, dayfirst=True)
    return flatfile


def merge_la_files(flat_output, dates, flatfile):
    """
    Looks for existing file of the same type and merges with new file if found
    """
    old_file = Path(flat_output, f"social_work_workforce_flatfile.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, parse_dates=dates, dayfirst=True)
        merged_df = pd.concat([flatfile, old_df], axis=0)
    else:
        merged_df = flatfile
    return merged_df


def deduplicate(flatfile, sort_order, dedup):
    """
    Sorts and removes duplicate records from merged files following schema
    """
    flatfile = flatfile.sort_values(sort_order, ascending=False, ignore_index=True)
    flatfile = flatfile.drop_duplicates(subset=dedup, keep="first")
    return flatfile


def remove_old_data(flatfile, years):
    """
    Removes data older than a specified number of years
    """
    year = pd.to_datetime("today").year
    month = pd.to_datetime("today").month
    if month <= 6:
        year = year - 1
    flatfile = flatfile[flatfile["YEAR"] >= year - years]
    return flatfile


def export_flatfile(flat_output, flatfile):
    """
    Writes the flatfile output as a csv
    """
    output_path = Path(flat_output, f"social_work_workforce_flatfile.csv")
    flatfile.to_csv(output_path, index=False)


def filter_flatfile(flatfile, filter):
    """
    Filters rows to specified events
    Removes redundant columns that relate to other types of event
    """
    filtered_flatfile = flatfile[flatfile["Type"] == filter]
    filtered_flatfile = filtered_flatfile.dropna(axis=1, how="all")
    return filtered_flatfile


