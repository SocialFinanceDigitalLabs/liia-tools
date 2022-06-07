import click as click

# dependencies for cleanfile()
from csdatatools.datasets.s903.lds_ssda903_clean import config
from csdatatools.datasets.s903.lds_ssda903_clean import parse
from csdatatools.datasets.s903.lds_ssda903_clean import populate
from csdatatools.datasets.s903.lds_ssda903_clean import filters
from csdatatools.datasets.s903.lds_ssda903_clean import degrade
from csdatatools.datasets.s903.lds_ssda903_clean import logger
from csdatatools.datasets.s903.lds_ssda903_clean import file_creator

# dependencies for la_agg()
from csdatatools.datasets.s903.lds_ssda903_la_agg import s903_la_agg

# dependencies for pan_agg()
from csdatatools.datasets.s903.lds_ssda903_pan_agg import s903_pan_agg
from csdatatools.datasets.s903.lds_ssda903_pan_agg import s903_pan_agg_config

# dependencies for suff_min()
from csdatatools.datasets.s903.lds_ssda903_suff_min import s903_suff_min
from csdatatools.datasets.s903.lds_ssda903_suff_min import s903_suff_min_config


@click.group()
def s903():
    """Functions for cleaning, minimising and aggregating SSDA903 files"""
    pass

@s903.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('la_name', type=str)
@click.argument('la_log_dir', type=click.Path(exists=True))
@click.argument('lds_log_dir', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def cleanfile(input, la_name, la_log_dir, lds_log_dir, output):
    """Cleans input SSDA903 csv files according to config and outputs cleaned csv files.
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that deposited the file
    'la_log_dir' should specify the path to the local authority's log folder
    'lds_log_dir' should specify the path to the LDS log folder
    'output' should specify the path to the output folder"""

    column_config = config.Config()["column_map"]
    la_config = config.Config()["la_map"]
    parse.log_config(lds_log_dir)
    stream = parse.findfiles(input)
    stream = parse.add_filename(stream)
    stream = populate.add_year_column(stream)
    stream = parse.parse_csv(stream, input=input)
    stream = config.add_table_name(stream)
    stream = config.inherit_table_name(stream)
    stream = config.match_config_to_cell(stream, config=column_config)
    stream = filters.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, config=la_config, la_name=la_name)
    stream = file_creator.coalesce_row(stream)
    stream = file_creator.create_tables(stream, la_name=la_name)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    stream = file_creator.save_tables(stream, output)
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