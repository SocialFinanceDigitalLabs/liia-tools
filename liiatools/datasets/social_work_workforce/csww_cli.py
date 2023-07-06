import logging
from pathlib import Path
import click
import yaml
import click_log

from liiatools.datasets.social_work_workforce import csww_main_functions

from liiatools.spec import common as common_asset_dir

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())


@click.group()
def csww():
    """
    Functions for creating CSWW Census sample file generator
    """
    pass

@csww.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--la_code",
    required=True,
    type=click.Choice(la_list, case_sensitive=False),
    help="A three letter code, specifying the local authority that deposited the file",
)
@click.option(
    "--la_log_dir",
    required=True,
    type=str,
    help="A string specifying the location that the log files for the LA should be output, usable by a pathlib Path function.",
)
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output directory location",
)
@click_log.simple_verbosity_option(log)
def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input social work workforce xml files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """
    output = csww_main_functions.cleanfile(input, la_code, la_log_dir, output)
    return output


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


@csww.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output directory location",
)
def la_agg(input, output):
    """
    Joins data from newly cleaned CSWW files (output of cleanfile()) to existing CSWW files data for the depositing local authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :return: None
    """
    csww_main_functions.la_agg(input, output)


@csww.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--la_code",
    required=True,
    type=click.Choice(la_list, case_sensitive=False),
    help="A three letter code, specifying the local authority that deposited the file",
)
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output directory location",
)
def pan_agg(input, la_code, output):
    """
    Joins data from newly merged social work workforce file (output of la-agg()) to existing pan-London social work workforce data
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param output: should specify the path to the output folder
    :return: None
    """
    csww_main_functions.pan_agg(input, la_code, output)