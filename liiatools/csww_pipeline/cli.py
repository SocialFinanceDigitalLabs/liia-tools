import logging
import click as click
import click_log
from fs import open_fs

from liiatools.common.reference import authorities
from liiatools.common.data import FileLocator

from .pipeline import process_session

log = logging.getLogger()
click_log.basic_config(log)


class FileLocatorParamType(click.ParamType):
    FileLocator(None, None)


file_locator = FileLocatorParamType()


@click.group()
def csww():
    """Functions for cleaning, minimising and aggregating CSWW files"""
    pass


@csww.command()
@click.option(
    "--la-code",
    "-c",
    required=True,
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
@click.option(
    "--public_input",
    "-pi",
    type=file_locator,
)
@click_log.simple_verbosity_option(log)
def pipeline(input, la_code, output, public_input):
    """
    Runs the full pipeline on a file or folder
    :param input: The path to the input folder
    :param la_code: A three-letter string for the local authority depositing the file
    :param output: The path to the output folder
    :param public_input: The FileLocator to public datasets needed for analysis
    :return: None
    """

    # Source FS is the filesystem containing the input files
    source_fs = open_fs(input)

    # Get the output filesystem
    output_fs = open_fs(output)

    process_session(source_fs, output_fs, la_code, public_input)
