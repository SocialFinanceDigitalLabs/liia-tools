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


def data_min(s903_df, minimise, table_name):
    """
    Performs additional data minimisation for pan-London sufficiency analysis in line with schema
    """
    if table_name in minimise.keys():
        s903_df = s903_df.drop(columns=minimise[table_name], axis=1)
    return s903_df


def export_suff_file(output, table_name, s903_df):
    """
    Writes file to output directory
    """
    output_path = Path(output, f"pan_London_SSDA903_{table_name}.csv")
    s903_df.to_csv(output_path, index=False)
