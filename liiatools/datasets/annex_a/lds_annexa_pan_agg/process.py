import logging
import pandas as pd
from pathlib import Path

log = logging.getLogger(__name__)


def split_file(input):
    """Reads merged LA xlsx file as dictionary of dataframes"""
    pan_dict = pd.read_excel(input, sheet_name=None, index_col=None, dtype=object)
    return pan_dict


def data_minimisation(pan_dict, minimise):
    """Reduces the sensitive information passed to the pan-London file according to schema"""
    for k in list(pan_dict):
        if k in ["List 10", "List 11"]:
            del pan_dict[k]
        else:
            df = pan_dict[k]
            df = df.drop(columns=minimise[k], axis=1)
            pan_dict[k] = df
    return pan_dict


def _merge_dfs(pan_dict, old_dict, la_name):
    """Deletes existing data for new LA from pan file
    Merges new LA data to pan file"""
    for k in pan_dict.keys():
        new_df = pan_dict[k]
        old_df = old_dict[k]
        old_df = old_df.drop(old_df[old_df["LA"] == la_name].index)
        merged_df = pd.concat([new_df, old_df], axis=0, ignore_index=True)
        pan_dict[k] = merged_df
    return pan_dict


def merge_agg_files(output, pan_dict, la_name):
    """Checks if pan file exists
    Passes old and new file to function to be merged"""
    output_file = Path(output, f"pan_London_Annex_A.xlsx")
    if output_file.is_file():
        old_dict = pd.read_excel(
            output_file, sheet_name=None, index_col=None, dtype=object
        )
        _merge_dfs(pan_dict, old_dict, la_name)
    return pan_dict


def convert_dates(pan_dict, dates):
    for k in pan_dict.keys():
        df = pan_dict[k]
        for date_field in dates[k]:
            df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y").dt.date
        pan_dict[k] = df
    return pan_dict


def export_file(output, pan_dict):
    """Writes output file as xlsx"""
    output_file = Path(output, f"pan_London_Annex_A.xlsx")
    with pd.ExcelWriter(output_file) as writer:
        for k in pan_dict.keys():
            df = pan_dict[k]
            df.to_excel(writer, sheet_name=k, index=False)
