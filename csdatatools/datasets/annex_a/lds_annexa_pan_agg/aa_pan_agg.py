import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

from csdatatools.datasets.annex_a.lds_annexa_pan_agg.aa_pan_agg_config import (minimise, dates)

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

#logging.basicConfig(filename=os.path.join(lds_log_dir, f"Annex_A_pan_London_agg_error_log_{start_time}.txt"),
#                    format="\n%(name)s: %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def split_file(input):
    try:
        dict = pd.read_excel(input, sheet_name=None, index_col=None)
        return dict
    except Exception as ex:
        log.exception(f"Error trying to split file: {input}")


def data_minimisation(dict, input):
    try:
        for key in list(dict):
            if key in ['List 10', 'List 11']:
                del dict[key]
            else:
                df = pd.DataFrame(dict[key])
                df = df.drop(columns=minimise[key], axis=1)
                dict[key] = df
        return dict
    except Exception as ex:
        log.exception(f"Error trying to minimise data in {input}")


def merge_agg_file(output, new_dict, la_name, input):
    try:
        file_list = Path(output).glob('*.xlsx')
        for file in file_list:
            old_dict = pd.read_excel(file, sheet_name=None, index_col=None)
            for k in new_dict.keys():
                new_df = pd.DataFrame(new_dict[k])
                old_df = pd.DataFrame(old_dict[k])
                old_df = old_df.drop(old_df[old_df['LA'] == la_name].index)
                merged_df = pd.concat([new_df, old_df], axis=0)
                new_dict[k] = merged_df
        return new_dict
    except Exception as ex:
        log.exception(f"Error trying to merge aggregated file for {input}")


def convert_dates(dict, input):
    try:
        for key in dict.keys():
            df = pd.DataFrame(dict[key])
            for date_field in dates[key]:
                df[date_field] = pd.to_datetime(df[date_field], format="%d/%m/%Y").dt.date
            dict[key] = df
        return dict
    except Exception as ex:
        log.exception(f"Error trying to convert dates for {input}")


def export_file(output, dict, input):
    try:
        output_path = Path(output, f"pan_London_Annex_A.xlsx")
        with pd.ExcelWriter(output_path) as writer:
            for k in dict.keys():
                df = dict[k]
                df.to_excel(writer, sheet_name=k, index=False)
    except Exception as ex:
        log.exception(f"Error trying to export file: {input}")