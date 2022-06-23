import click as click
from pathlib import Path
import yaml

from sfdata_stream_parser.parser import openpyxl
from liiatools.datasets.annex_a.lds_annexa_clean import (
    configuration,
    cleaner,
    degrade,
    logger,
    populate,
    file_creator,
)
from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import flip_dict
from sfdata_stream_parser.filters.column_headers import promote_first_row

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())


@click.group()
def annex_a():
    """Functions for cleaning, minimising and aggregating Annex A files"""
    pass


@annex_a.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--la_code",
    type=click.Choice(la_list, case_sensitive=False),
    help="A LA code, specifying the local authority that deposited the file",
)
@click.option(
    "--la_log_dir",
    default="empty",
    type=str,
    help="A string specifying the location that the log files for the LA should be output, usable by a pathlib Path function.",
)
@click.option(
    "--o",
    "output",
    default="empty",
    type=str,
    help="A string specifying the output directory location",
)
def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input Annex A xlsx files according to config and outputs cleaned xlsx files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a string of the name of the local authority that deposited the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    filename = Path(input).resolve().stem
    config = configuration.Config()
    la_name = flip_dict(config["data_codes"])[la_code]

    # Open & Parse file
    stream = openpyxl.parse_sheets(input)
    stream = [ev.from_event(ev, la_code=la_code, filename=filename) for ev in stream]
    stream = promote_first_row(stream)

    # Configure Stream
    stream = configuration.configure_stream(stream, config)

    # Clean stream
    stream = cleaner.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, la_code=la_code)

    # Output result
    stream = file_creator.save_stream(stream, la_name, output)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    list(stream)