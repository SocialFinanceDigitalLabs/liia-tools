import click as click
from pathlib import Path
import yaml

# dependencies for cleanfile()
from liiatools.datasets.s903.lds_ssda903_clean import (
    configuration as clean_config,
    parse,
    populate,
    filters,
    degrade,
    logger,
    file_creator
)

# dependencies for la_agg()
from liiatools.datasets.s903.lds_ssda903_la_agg import configuration as agg_config
from liiatools.datasets.s903.lds_ssda903_la_agg import process as agg_process

# dependencies for pan_agg()
from liiatools.datasets.s903.lds_ssda903_pan_agg import configuration as pan_config
from liiatools.datasets.s903.lds_ssda903_pan_agg import process as pan_process

# dependencies for suff_min()
#from liiatools.datasets.s903.lds_ssda903_suff_min import ( s903_suff_min, s903_suff_min_config )

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import flip_dict

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())

@click.group()
def s903():
    """Functions for cleaning, minimising and aggregating SSDA903 files"""
    pass

@s903.command()
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
def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input SSDA903 csv files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = clean_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]

    # Open & Parse file
    stream = parse.parse_csv(input=input)
    stream = populate.add_year_column(stream)

    # Configure stream
    stream = clean_config.configure_stream(stream, config)

    # Clean stream
    stream = filters.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, la_code=la_code)

    # Output result
    stream = file_creator.save_stream(stream, la_name=la_name, output=output)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    list(stream)


@s903.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
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
def la_agg(input, la_log_dir, output):
    """
    Joins data from newly cleaned SSDA903 file (output of cleanfile()) to existing SSDA903 data for the depositing local authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Open file as DataFrame and match file type
    s903_df = agg_process.read_file(input)
    column_names = config["column_names"]
    table_name = agg_process.match_load_file(s903_df, column_names)

    # Merge file with existing file of the same type in LA output folder
    s903_df = agg_process.merge_la_files(output, s903_df, table_name)

    # De-duplicate and remove old data according to schema
    dates = config["dates"]
    s903_df = agg_process.convert_datetimes(s903_df, dates, table_name)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    s903_df = agg_process.deduplicate(s903_df, table_name, sort_order, dedup)
    s903_df = agg_process.remove_old_data(s903_df, years=6)

    # If file still has data, after removing old data: log errors, re-format and export merged file
    if len(s903_df) > 0:
        agg_process.log_missing_years(s903_df, table_name, la_log_dir)
        s903_df = agg_process.convert_dates(s903_df, dates, table_name)
        agg_process.export_la_file(output, table_name, s903_df)


@s903.command()
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
    Joins data from newly merged SSDA903 file (output of la-agg()) to existing pan-London SSDA903 data
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = pan_config.Config()
    
    # Read file and match type
    s903_df = pan_process.read_file(input)
    column_names = config["column_names"]
    table_name = pan_process.match_load_file(s903_df, column_names)

    # Remove unwanted datasets and merge wanted with existing output
    pan_data_kept = config["pan_data_kept"]
    if table_name in pan_data_kept:
        la_name = flip_dict(config["data_codes"])[la_code]
        s903_df = pan_process.merge_agg_files(output, table_name, s903_df, la_name)
        pan_process.export_pan_file(output, table_name, s903_df)