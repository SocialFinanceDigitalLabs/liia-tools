import click as click
import pandas as pd

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
            file_loc = f"{root}\\{file}"
            if file_loc.endswith(".csv"):
                df = pd.read_csv(file_loc, sep=",", header=0)
                df.to_csv(file_loc, index=False)
