import logging

import click as click
import click_log
from fs import open_fs

from liiatools.common.reference import authorities

from .pipeline import process_session

log = logging.getLogger()
click_log.basic_config(log)


@click.group()
def school_census():
    """Functions for cleaning, minimising and aggregating school census files"""
    pass


@school_census.command()
@click.option(
    "--la-code",
    "-c",
    required=True,
    type=click.Choice(authorities.codes, case_sensitive=False),
    help="Local authority code",
)
@click.option(
    "--term",
    "-t",
    required=True,
    type=click.Choice(["Spring, Autumn/Summer"], case_sensitive=True),
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
def pipeline(input, la_code, output, term):
    """Runs the full pipeline on a file or folder"""

    # Source FS is the filesystem containing the input files
    source_fs = open_fs(input)

    # Get the output filesystem
    output_fs = open_fs(output)

    process_session(source_fs, output_fs, la_code, term)
