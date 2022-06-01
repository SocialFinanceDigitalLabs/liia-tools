import os
from pathlib import Path
import pandas as pd
from datetime import date, datetime
import logging

from csdatatools.datasets.annex_a.lds_annexa_la_agg.aa_la_agg_config import (dates, dedup, index_date)

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

#logging.basicConfig(filename=os.path.join(lds_log_dir, f"Annex_A_la_agg_error_log_{start_time}.txt"),
#                    format="\n%(name)s: %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def split_file(input):
    dict = pd.read_excel(input, sheet_name=None, index_col=None)
    return dict


def merge_la_files(output, new_dict, input):
    file_list = Path(output).glob('*.xlsx')
    for file in file_list:
        old_dict = pd.read_excel(file, sheet_name=None, index_col=None)
        for k in new_dict.keys():
            new_df = pd.DataFrame(new_dict[k])
            old_df = pd.DataFrame(old_dict[k])
            merged_df = pd.concat([new_df, old_df], axis=0)
            new_dict[k] = merged_df
    return new_dict


def deduplicate(dict, input):
    if dict:
        for key in dict.keys():
            df = pd.DataFrame(dict[key])
            df = df.drop_duplicates(subset=dedup[key], keep = 'first')
            dict[key] = df
    return dict



def convert_dates(dict, input):
    if dict:
        for key in dict.keys():
            df = pd.DataFrame(dict[key])
            for date_field in dates[key]:
                df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y").dt.date
            dict[key] = df
    return dict


def remove_years(d, years):
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d + (date(d.year - years, 1, 1) - date(d.year, 1, 1))



def remove_old_data(dict, input):
    today = pd.to_datetime('today')
    six_years_ago = remove_years(today, 6)
    thirty_years_ago = remove_years(today, 30)
    if dict:
        for key in dict.keys():
            df = pd.DataFrame(dict[key])
            if key == 'List 9':
                df = df[df[index_date[key]] >= thirty_years_ago]
            elif key == 'List 10':
                df = df[(df[index_date[key]] >= six_years_ago).any(axis=1)]
            else:
                df = df[(df[index_date[key]] >= six_years_ago) | (df[index_date[key]].isnull())]
            dict[key] = df
    return dict


def export_file(output, dict, input):
    output_path = Path(output, f"AnnexA_merged.xlsx")
    with pd.ExcelWriter(output_path) as writer:
        if dict:
            for k in dict.keys():
                df = dict[k]
                df.to_excel(writer, sheet_name=k, index=False)