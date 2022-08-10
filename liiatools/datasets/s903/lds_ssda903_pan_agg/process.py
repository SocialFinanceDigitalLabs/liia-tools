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


def _merge_dfs(s903_df, old_df, la_name):
    """
    Deletes existing data for new LA from pan file
    Merges new LA data to pan file
    """
    old_df = old_df.drop(old_df[old_df["LA"] == la_name].index)
    s903_df = pd.concat([s903_df, old_df], axis=0, ignore_index=True)
    return s903_df


def merge_agg_files(output, table_name, s903_df, la_name):
    """
    Checks if pan file exists
    Passes old and new file to function to be merged
    """
    output_file = Path(output, f"pan_London_SSDA903_{table_name}.csv")
    if output_file.is_file():
        old_df = pd.read_csv(output_file, index_col=None)
        s903_df = _merge_dfs(s903_df, old_df, la_name)
    return s903_df


def export_pan_file(output, table_name, s903_df):
    """
    Writes file to output directory
    """
    output_path = Path(output, f"pan_London_SSDA903_{table_name}.csv")
    s903_df.to_csv(output_path, index=False)
