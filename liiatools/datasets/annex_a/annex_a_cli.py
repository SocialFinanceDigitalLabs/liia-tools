import click as click
from pathlib import Path

# Dependencies for cleanfile():
from sfdata_stream_parser.parser import openpyxl

from liiatools.datasets.annex_a.lds_annexa_clean import (
    configuration,
    cleaner,
    degrade,
    logger,
    populate,
    file_creator,
)

from sfdata_stream_parser.filters.column_headers import promote_first_row


@click.group()
def annex_a():
    """Functions for cleaning, minimising and aggregating Annex A files"""
    pass


@annex_a.command()
@click.argument("input", type=click.Path(exists=True))
@click.argument("la_name", type=str)
@click.argument("la_log_dir", type=click.Path(exists=True))
@click.argument("lds_log_dir", type=click.Path(exists=True))
@click.argument("output", type=click.Path(exists=True))
def cleanfile(input, la_name, la_log_dir, lds_log_dir, output):
    """Cleans input Annex A xlsx files according to config and outputs cleaned xlsx files.
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that deposited the file
    'la_log_dir' should specify the path to the local authority's log folder
    'lds_log_dir' should specify the path to the LDS log folder
    'output' should specify the path to the output folder"""

    # Configuration
    filename = Path(input).resolve().stem
    config = configuration.Config()
    sheet_config = config["datasources"]
    cell_config = config["data_config"]
    la_config = config["data_codes"]

    stream = openpyxl.parse_sheets(input)

    stream = [ev.from_event(ev, la_name=la_name, filename=filename) for ev in stream]
    stream = promote_first_row(stream)

    stream = configuration.add_sheet_name(stream, config=sheet_config)
    stream = configuration.inherit_property(stream, "sheet_name")
    stream = configuration.inherit_property(stream, "column_headers")
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
