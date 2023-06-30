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


def _merge_dfs(s251_df, old_df, la_name):
    """
    Deletes existing data for new LA from pan file
    Merges new LA data to pan file
    """
    old_df = old_df.drop(old_df[old_df["LA"] == la_name].index)
    s251_df = pd.concat([s251_df, old_df], axis=0, ignore_index=True)
    return s251_df


def merge_agg_files(output, s251_df, la_name):
    """
    Checks if pan file exists
    Passes old and new file to function to be merged
    """
    output_file = Path(output, f"pan_London_S251.csv")
    if output_file.is_file():
        old_df = pd.read_csv(output_file, index_col=None)
        s251_df = _merge_dfs(s251_df, old_df, la_name)
    return s251_df


def export_pan_file(output, s251_df):
    """
    Writes file to output directory
    """
    output_path = Path(output, f"pan_London_S251.csv")
    s251_df.to_csv(output_path, index=False)
