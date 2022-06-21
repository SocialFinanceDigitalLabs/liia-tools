import click as click
from pathlib import Path
import yaml

# dependencies for cleanfile()
from liiatools.datasets.s903.lds_ssda903_clean import ( configuration, parse, populate, filters, degrade, logger, file_creator )

# dependencies for la_agg()
#from liiatools.datasets.s903.lds_ssda903_la_agg import s903_la_agg

# dependencies for pan_agg()
#from liiatools.datasets.s903.lds_ssda903_pan_agg import ( s903_pan_agg, s903_pan_agg_config )

# dependencies for suff_min()
#from liiatools.datasets.s903.lds_ssda903_suff_min import ( s903_suff_min, s903_suff_min_config )

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import flip_dict
COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent

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
    default="empty",
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
    """Cleans input SSDA903 csv files according to config and outputs cleaned csv files.
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that deposited the file
    'la_log_dir' should specify the path to the local authority's log folder
    'lds_log_dir' should specify the path to the LDS log folder
    'output' should specify the path to the output folder"""

    # Configuration
    config = configuration.Config()
    la_name = flip_dict(config["data_codes"])[la_code]

    # Open & Parse file
    stream = parse.parse_csv(input=input)
    stream = populate.add_year_column(stream)

    # Configure stream
    stream = configuration.configure_stream(stream, config)

    # Clean stream
    stream = filters.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, la_code=la_code)

    # Output result
    stream = file_creator.save_stream(stream, la_name=la_name, output=output)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    list(stream)
