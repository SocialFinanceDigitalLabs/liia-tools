from pathlib import Path
import logging
import pandas as pd

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


def _merge_dfs(csww_df, old_df, la_name):
    """
    Deletes existing data for new LA from pan file
    Merges new LA data to pan file
    """
    old_df = old_df.drop(old_df[old_df["LA"] == la_name].index)
    csww_df = pd.concat([csww_df, old_df], axis=0, ignore_index=True)
    return csww_df


def merge_agg_files(output, table_name, csww_df, la_name):
    """
    Checks if pan file exists
    Passes old and new file to function to be merged
    """
    output_file = Path(output, f"pan_London_CSWW_{table_name}.csv")
    if output_file.is_file():
        old_df = pd.read_csv(output_file, index_col=None)
        csww_df = _merge_dfs(csww_df, old_df, la_name)
    return csww_df


def export_pan_file(output, table_name, csww_df):
    """
    Writes file to output directory
    """
    output_path = Path(output, f"pan_London_CSWW_{table_name}.csv")
    csww_df.to_csv(output_path, index=False)
