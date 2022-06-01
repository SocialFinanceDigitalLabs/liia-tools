import click as click
from pathlib import Path

# Dependencies for cleanfile():
from sfdata_stream_parser.parser import openpyxl

from csdatatools.datasets.annex_a.lds_annexa_clean import configuration
from csdatatools.datasets.annex_a.lds_annexa_clean import parse
from csdatatools.datasets.annex_a.lds_annexa_clean import cleaner
from csdatatools.datasets.annex_a.lds_annexa_clean import degrade
from csdatatools.datasets.annex_a.lds_annexa_clean import logger
from csdatatools.datasets.annex_a.lds_annexa_clean import populate
from csdatatools.datasets.annex_a.lds_annexa_clean import file_creator

from sfdata_stream_parser.filters.column_headers import promote_first_row

# Dependencies for la_agg():
from csdatatools.datasets.annex_a.lds_annexa_la_agg import aa_la_agg

# Dependencies for pan_agg():
from csdatatools.datasets.annex_a.lds_annexa_pan_agg import aa_pan_agg

@click.group()
def annex_a():
    """Functions for cleaning, minimising and aggregating Annex A files"""
    pass

@annex_a.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('la_name', type=str)
@click.argument('la_log_dir', type=click.Path(exists=True))
@click.argument('lds_log_dir', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def cleanfile(input, la_name, la_log_dir, lds_log_dir, output):
    """Cleans input Annex A xlsx files according to config and outputs cleaned xlsx files.
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that deposited the file
    'la_log_dir' should specify the path to the local authority's log folder
    'lds_log_dir' should specify the path to the LDS log folder
    'output' should specify the path to the output folder"""

    filename = Path(input).resolve().stem
    config = configuration.Config()
    sheet_config = config["datasources"]
    cell_config = config["data_config"]
    la_config = config["data_codes"]

    stream = openpyxl.parse_sheets(input)

    stream = [ev.from_event(ev, la_name=la_name, filename=filename) for ev in stream]
    stream = promote_first_row(stream)

    stream = configuration.add_sheet_name(stream, config=sheet_config)
    stream = configuration.inherit_property(stream, 'sheet_name')
    stream = configuration.inherit_property(stream, 'column_headers')
    stream = configuration.identify_cell_header(stream)

    stream = configuration.convert_column_header_to_match(stream, config=sheet_config)
    stream = configuration.match_category_config_to_cell(stream, config=cell_config)
    stream = configuration.match_other_config_to_cell(stream, config=sheet_config)
    stream = cleaner.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, config=la_config, la_name=la_name)
    stream = file_creator.coalesce_row(stream)
    stream = file_creator.filter_rows(stream)
    stream = file_creator.create_tables(stream, la_name=la_name)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    stream = file_creator.save_tables(stream, output=output)
    list(stream)


@annex_a.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def la_agg(input, output):
    '''Aggregates all Annex A files that have been uploaded by a local authority to a single file
    'input' should specify the input file location of the new file to be processed, including file name and suffix, and be usable by a Path function
    'output' should specify the local authority's output directory, where the aggregated file should be written to, and be usable by a Path function'''

    stream = aa_la_agg.split_file(input)
    stream = aa_la_agg.merge_la_files(output, stream, input)
    stream = aa_la_agg.deduplicate(stream, input)
    stream = aa_la_agg.convert_dates(stream, input)
    stream = aa_la_agg.remove_old_data(stream, input)
    aa_la_agg.export_file(output, stream, input)


@annex_a.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('la_name', type=str)
@click.argument('output', type=click.Path(exists=True))
def pan_agg(input, output, la_name):
    '''Aggregates all Annex A files for all local authorities and outputs to Structural Inequalities Benchmarking folder
    'input' should specify the input file location of the new LA to be processed, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority whose data needs to be aggregated
    'output' should specify the local authority's output directory, where the aggregated file should be written to, and be usable by a Path function'''

    stream = aa_pan_agg.split_file(input)
    stream = aa_pan_agg.data_minimisation(stream, input)
    stream = aa_pan_agg.merge_agg_file(output, stream, la_name, input)
    stream = aa_pan_agg.convert_dates(stream, input)
    aa_pan_agg.export_file(output, stream, input)