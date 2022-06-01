import click as click

# Dependencies for cleanfile()
from sfdata_stream_parser.parser.xml import parse

from liiatools.datasets.cincensus.config import Schema
from liiatools.datasets.cincensus import filters

from liia_pipeline.datasets.cin_census.lds_cin_clean import cin_record
from liia_pipeline.datasets.cin_census.lds_cin_clean import file_creator


@click.group()
def cin():
    """Functions for cleaning, minimising and aggregating CIN Census files"""
    pass


@cin.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('la_name', type=str)
@click.argument('la_log_dir', type=click.Path(exists=True))
@click.argument('lds_log_dir', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def cleanfile(input, la_name, la_log_dir, lds_log_dir, output):
    """Cleans an input CIN Census xml file according to config and outputs a cleaned csv file.
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that deposited the file
    'la_log_dir' should specify the path to the local authority's log folder
    'lds_log_dir' should specify the path to the LDS log folder
    'output' should specify the path to the output folder"""

    stream = parse(input)
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_config(stream, Schema().fields_with_prefix(['Message', 'Children', 'Child']))
    stream = filters.clean(stream)
    stream = cin_record.message_collector(stream)
    data = cin_record.export_table(stream)
    data = file_creator.add_fields(input, data, la_name)
    file_creator.export_file(input, output, data)