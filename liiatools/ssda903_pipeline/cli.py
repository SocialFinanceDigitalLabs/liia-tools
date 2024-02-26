import logging

import click as click
import click_log
from fs import open_fs

from liiatools.common.reference import authorities

from .pipeline import process_session

log = logging.getLogger()
click_log.basic_config(log)


@click.group()
def s903():
    """Functions for cleaning, minimising and aggregating SSDA903 files"""
    pass


@s903.command()
@click.option(
    "--la-code",
    "-c",
    type=click.Choice(authorities.codes, case_sensitive=False),
    help="Local authority code",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(file_okay=False, writable=True),
    help="Output folder",
)
@click.option(
    "--input",
    "-i",
    type=click.Path(exists=True, file_okay=False, readable=True),
)
@click_log.simple_verbosity_option(log)
def pipeline(input, output, la_code=None):
    """Runs the full pipeline on a file or folder"""

    # Source FS is the filesystem containing the input files
    source_fs = open_fs(input)

    # Get the output filesystem
    output_fs = open_fs(output)

    process_session(source_fs, output_fs, la_code)
