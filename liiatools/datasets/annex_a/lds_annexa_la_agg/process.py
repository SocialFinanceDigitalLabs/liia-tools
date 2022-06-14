from pathlib import Path
import pandas as pd
from datetime import date
import logging

log = logging.getLogger(__name__)


def split_file(input):
    '''Reads xlsx file as dictionary of dataframes'''
    dict = pd.read_excel(input, sheet_name=None, index_col=None, dtype=object)
    return dict


def sort_dict(dict, sort_order):
    '''Sorts the imported dict by List to ensure consistency'''
    for k in dict.keys():
        df = dict[k]
        df = df[sort_order[k]]
        dict[k] = df
    return dict


def _merge_dfs(new_dict, old_dict):
    for k in new_dict.keys():
        new_df = new_dict[k]
        old_df = old_dict[k]
        merged_df = pd.concat([new_df, old_df], axis=0, ignore_index=True)
        new_dict[k] = merged_df
    return new_dict


def merge_la_files(output, new_dict):
    file_list = Path(output).glob('*.xlsx')
    for file in file_list:
        old_dict = pd.read_excel(file, sheet_name=None, index_col=None, dtype=object)
        _merge_dfs(new_dict, old_dict)
    return new_dict


def deduplicate(dict, dedup):
    for key in dict.keys():
        df = dict[key]
        df = df.drop_duplicates(subset=dedup[key], keep = 'first')
        dict[key] = df
    return dict


def convert_datetimes(dict, dates):
    for key in dict.keys():
        df = dict[key]
        for date_field in dates[key]:
            df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y")
        dict[key] = df
    return dict


def _remove_years(d, years):
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d + (date(d.year - years, 1, 1) - date(d.year, 1, 1))


def remove_old_data(dict, index_date):
    today = pd.to_datetime('today')
    six_years_ago = _remove_years(today, 6)
    thirty_years_ago = _remove_years(today, 30)
    for key in dict.keys():
        df = dict[key]
        if key == 'List 9':
            df = df[df[index_date[key]] >= thirty_years_ago]
        elif key == 'List 10':
            df = df[(df[index_date[key]] >= six_years_ago).any(axis=1)]
        else:
            df = df[(df[index_date[key]] >= six_years_ago) | (df[index_date[key]].isnull())]
        dict[key] = df
    return dict


def convert_dates(dict, dates):
    for key in dict.keys():
        df = dict[key]
        for date_field in dates[key]:
            df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y").dt.date
        dict[key] = df
    return dict


def export_file(output, dict):
    output_path = Path(output, f"AnnexA_merged.xlsx")
    with pd.ExcelWriter(output_path) as writer:
        for k in dict.keys():
            df = dict[k]
            df.to_excel(writer, sheet_name=k, index=False)