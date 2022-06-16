from pathlib import Path
import pandas as pd
from datetime import date
import logging

log = logging.getLogger(__name__)


def split_file(input):
    '''Reads xlsx file as dictionary of dataframes'''
    dict = pd.read_excel(input, sheet_name=None, index_col=None, dtype=object)
    return dict


def sort_dict(aa_dict, sort_order):
    '''Sorts the imported dict by List to ensure consistency'''
    for k in aa_dict.keys():
        df = aa_dict[k]
        df = df[sort_order[k]]
        aa_dict[k] = df
    return aa_dict


def _merge_dfs(aa_dict, old_dict):
    for k in aa_dict.keys():
        new_df = aa_dict[k]
        old_df = old_dict[k]
        merged_df = pd.concat([new_df, old_df], axis=0, ignore_index=True)
        aa_dict[k] = merged_df
    return aa_dict


def merge_la_files(output, aa_dict):
    file_list = Path(output).glob('*.xlsx')
    for file in file_list:
        old_dict = pd.read_excel(file, sheet_name=None, index_col=None, dtype=object)
        _merge_dfs(aa_dict, old_dict)
    return aa_dict


def deduplicate(aa_dict, dedup):
    for key in aa_dict.keys():
        df = aa_dict[key]
        df = df.drop_duplicates(subset=dedup[key], keep = 'first')
        aa_dict[key] = df
    return aa_dict


def convert_datetimes(aa_dict, dates):
    for key in aa_dict.keys():
        df = aa_dict[key]
        for date_field in dates[key]:
            df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y")
        aa_dict[key] = df
    return aa_dict


def _remove_years(d, years):
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d + (date(d.year - years, 1, 1) - date(d.year, 1, 1))


def remove_old_data(aa_dict, index_date):
    today = pd.to_datetime('today')
    six_years_ago = _remove_years(today, 6)
    thirty_years_ago = _remove_years(today, 30)
    for key in aa_dict.keys():
        df = aa_dict[key]
        if key == 'List 9':
            df = df[df[index_date[key]] >= thirty_years_ago]
        elif key == 'List 10':
            df = df[(df[index_date[key]] >= six_years_ago).any(axis=1)]
        else:
            df = df[(df[index_date[key]] >= six_years_ago) | (df[index_date[key]].isnull())]
        aa_dict[key] = df
    return aa_dict


def convert_dates(aa_dict, dates):
    for key in aa_dict.keys():
        df = aa_dict[key]
        for date_field in dates[key]:
            df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y").dt.date
        aa_dict[key] = df
    return aa_dict


def export_file(output, aa_dict):
    output_path = Path(output, f"AnnexA_merged.xlsx")
    with pd.ExcelWriter(output_path) as writer:
        for k in aa_dict.keys():
            df = aa_dict[k]
            df.to_excel(writer, sheet_name=k, index=False)