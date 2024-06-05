import click as click
import pandas as pd

import csv
import os
import click_log
import logging


log = logging.getLogger()
click_log.basic_config(log)


@click.group()
def reformat_csv():
    """Function for reformatting csvs"""
    pass


@reformat_csv.command()
@click.option(
    "--input",
    "-i",
    type=click.Path(exists=True, file_okay=False, readable=True),
)
@click_log.simple_verbosity_option(log)
def process(input):
    for root, dirs, files in os.walk(input):
        for file in files:
            new_data = []
            file_loc = f"{root}\\{file}"
            if file_loc.endswith(".csv"):
                try:
                    pd.read_csv(file_loc)
                    pass
                except pd.errors.ParserError:
                    with open(file_loc, "r") as f:
                        data = f.read().splitlines()
                        headers_len = len(data[0].split(","))
                        row_no = 0
                        for row in data:
                            row_no += 1
                            split_row = row.split(",")
                            split_row_len = len(split_row)
                            if split_row_len == headers_len:
                                new_data.append(split_row)
                            elif split_row_len > headers_len:
                                extra_columns = split_row[headers_len:]
                                if not any(extra_columns):
                                    new_data.append(split_row[:headers_len-split_row_len])
                                else:
                                    raise Exception(f"Non blank extra columns in {file_loc} on row: {row_no}")

            if new_data:
                with open(file_loc[:-4] + "_test.csv", "w", newline="") as f:
                    csvwriter = csv.writer(f, delimiter=',')
                    csvwriter.writerows(new_data)
