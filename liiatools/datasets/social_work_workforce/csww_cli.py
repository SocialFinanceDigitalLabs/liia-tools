import logging
import click as click
import click_log
from pathlib import Path
import yaml
from datetime import datetime

from liiatools.datasets.social_work_workforce import csww_main_functions

from liiatools.spec import common as common_asset_dir
log = logging.getLogger()
click_log.basic_config(log)

# COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# # Get all the possible LA codes that could be used
# with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
#     la_list = list(yaml.full_load(las)["data_codes"].values())
# this is in csww main

@click.group()
def csww():
    """
    Functions for creating CSWW Census sample file generator
    """
    pass


@csww.command()
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output file location, including the file name and suffix",
)
@click_log.simple_verbosity_option(log)
def generate_sample(output: str):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :return: .xml sample file in desired location
    """
    output = csww_main_functions.generate_sample(output)
    return output
