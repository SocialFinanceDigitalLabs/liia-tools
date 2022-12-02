import click as click
from pathlib import Path
import yaml
import logging
import click_log

from sfdata_stream_parser.parser import openpyxl
from liiatools.datasets.annex_a.lds_annexa_clean import (
    configuration as clean_config,
    cleaner,
    degrade,
    logger,
    populate,
    file_creator,
)
from liiatools.datasets.annex_a.lds_annexa_la_agg import configuration as agg_config
from liiatools.datasets.annex_a.lds_annexa_la_agg import process as agg_process

from liiatools.datasets.annex_a.lds_annexa_pan_agg import configuration as pan_config
from liiatools.datasets.annex_a.lds_annexa_pan_agg import process as pan_process

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
)
from sfdata_stream_parser.filters.column_headers import promote_first_row

log = logging.getLogger()
click_log.basic_config(log)

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
    Cleans input Annex A xlsx files according to config and outputs cleaned xlsx files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    filename = Path(input).resolve().stem
    config = clean_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]
    if (
        check_file_type(
            input,
            file_types=[".xlsx", ".xlsm"],
            supported_file_types=supported_file_types,
            la_log_dir=la_log_dir,
        )
        == "incorrect file type"
    ):
        return

    # Open & Parse file
    stream = openpyxl.parse_sheets(input)
    stream = [ev.from_event(ev, la_code=la_code, filename=filename) for ev in stream]
    stream = promote_first_row(stream)

    # Configure Stream
    stream = clean_config.configure_stream(stream, config)

    # Clean stream
    stream = cleaner.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, la_code=la_code)

    # Output result
    stream = file_creator.save_stream(stream, la_name, output)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    list(stream)


@annex_a.command()
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
    Joins data from newly cleaned Annex A file (output of cleanfile()) to existing Annex A data for the depositing local authority
    :param input: a string specifying the input file location of the newly cleaned file, including file name and suffix, usable by a Path function
    :param output: a string specifying the local authority's Annex A output directory, usable by a Path function
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Open cleaned file as dictionary
    aa_dict = agg_process.split_file(input)

    # Merge with existing LA file, if any
    list_columns = config["sort_order"]
    aa_dict = agg_process.sort_dict(aa_dict, list_columns=list_columns)
    aa_dict = agg_process.merge_la_files(output, aa_dict)

    # Remove duplicate data and data older than retention period
    dedup = config["dedup"]
    dates = config["dates"]
    index_date = config["index_date"]
    aa_dict = agg_process.deduplicate(aa_dict, dedup=dedup)
    aa_dict = agg_process.convert_datetimes(aa_dict, dates=dates)
    aa_dict = agg_process.remove_old_data(aa_dict, index_date=index_date)
    aa_dict = agg_process.convert_dates(aa_dict, dates=dates)

    # Output result
    agg_process.export_file(output, aa_dict)


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
    Merges data from newly merged Annex A file (output of la_agg()) to existing pan-London Annex A data
    :param input: a string specifying the input file location of the newly merged file, including file name and suffix, usable by a Path function
    :param la_code: should be a three-letter string for the local authority whose data is to be merged
    :param output: a string specifying the pan-London Annex A output directory, usable by a Path function
    :return: None
    """

    # Configuration
    config = pan_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]

    # Open merged file as dictionary
    pan_dict = pan_process.split_file(input)

    # Minimise data in merged file following schema
    minimise = config["minimise"]
    pan_dict = pan_process.data_minimisation(pan_dict, minimise=minimise)

    # Merge with existing pan-London file, if any
    pan_dict = pan_process.merge_agg_files(output, pan_dict, la_name)

    # Format and write file to folder
    dates = config["dates"]
    pan_dict = pan_process.convert_dates(pan_dict, dates=dates)
    pan_process.export_file(output, pan_dict)
