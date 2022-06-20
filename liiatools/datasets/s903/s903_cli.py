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


@s903.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('la_log_dir', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def la_agg(input, la_log_dir, output):
    '''Aggregates all SSDA903 files of a given type that have been uploaded by a local authority
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_log_dir' should specify the path to the local authority's log folder    
    'output' should specify the local authority's output directory, where the aggregated file should be written to, and be usable by a Path function'''

    stream = s903_la_agg.la_read_file(input)
    table_name = s903_la_agg.la_match_load_file(stream)
    stream = s903_la_agg.merge_la_files(output, table_name, stream)
    stream = s903_la_agg.convert_dates(stream, table_name)
    stream = s903_la_agg.deduplicate(stream, table_name)
    stream = s903_la_agg.remove_old_data(stream)
    stream = s903_la_agg.log_missing_years(stream, table_name, la_log_dir)
    s903_la_agg.export_la_file(output, table_name, stream)


@s903.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('la_name', type=str)
@click.argument('output', type=click.Path(exists=True))
def pan_agg(input, la_name, output):
    '''Aggregates all SSDA903 files of a given type for all local authorities and outputs to Structural Inequalities Benchmarking folder
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that corresponds to the input file
    'output' should specify the LIIA Analysis folder for the Structural Inequalities Benchmarking project, and be usable by a Path function'''

    stream = s903_pan_agg.pan_read_file(input)
    table_name = s903_pan_agg.pan_match_load_file(stream)
    if table_name in s903_pan_agg_config.pan_data_kept:
        stream = s903_pan_agg.merge_agg_files(output, table_name, la_name, stream)
        s903_pan_agg.export_pan_file(output, table_name, stream)


@s903.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def suff_min(input, output):
    '''Minimises the output of pan_agg() and outputs this to Pan-London Sufficiency Analysis folder
    'input' should specify the input file location of the file in the Pan-London Sufficiency Analysis folder, including file name and suffix, and be usable by a Path function
    'output' should specify the LIIA Analysis folder for the Pan-London Sufficiency Analysis project, and be usable by a Path function'''
    
    stream = s903_suff_min.suff_read_file(input)
    table_name = s903_suff_min.suff_match_load_file(stream)
    if table_name in s903_suff_min_config.suff_data_kept:
        stream = s903_suff_min.data_min(stream, table_name)
        s903_suff_min.export_suff_file(output, table_name, stream)