import logging

import click as click
import click_log
from fs import open_fs

from liiatools.common.reference import authorities

from .pipeline import (
    create_session_folder,
    open_archive,
    process_files,
    create_current_view,
    create_reports
)

log = logging.getLogger()
click_log.basic_config(log)


@click.group()
def s903():
    """Functions for cleaning, minimising and aggregating SSDA903 files"""
    pass


@click.option(
    "--input-location",
    "-i",
    required=True,
    type=click.Path(exists=True, file_okay=False, readable=True),
    help="Input folder",
)
@click.option(
    "--output-location",
    "-o",
    required=True,
    type=click.Path(file_okay=False, writable=True),
    help="Output folder",
)
@s903.command()
@click.option(
    "--input-la-code",
    "-c",
    required=False,
    type=click.Choice(authorities.codes, case_sensitive=False),
    help="Local authority code",
)
@click_log.simple_verbosity_option(log)
def pipeline(input_location, output_location, input_la_code=None):
    """Runs the full pipeline on a file or folder"""
    incoming_folder = open_fs(input_location)
    process_folder = open_fs(output_location)

    session_folder, session_id, incoming_files = create_session_folder(process_folder, incoming_folder)
    archive = open_archive(session_id, process_folder)

    process_files(
        session_folder, incoming_files, archive, session_id, input_la_code
    )

    current_folder = create_current_view(archive, process_folder)

    create_reports(current_folder, process_folder)
