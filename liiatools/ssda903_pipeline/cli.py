import logging
from datetime import datetime
from pathlib import Path

import click as click
import click_log
from fs import open_fs

from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.data import ErrorContainer
from liiatools.common.reference import authorities
from liiatools.common.transform import degrade_data, enrich_data, prepare_export

from .spec import load_pipeline_config, load_schema
from .stream_pipeline import task_cleanfile

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
@click_log.simple_verbosity_option(log)
def pipeline(input, la_code, output):
    """Runs the full pipeline on a file or folder"""

    pipeline_config = load_pipeline_config()

    # Source FS is the filesystem containing the input files
    source_fs = open_fs(input)

    # Get the output filesystem
    output_fs = open_fs(output)

    # Create session folder and move all files into it
    session_folder, locator_list = pl.create_session_folder(source_fs, output_fs)

    # Archive holds rolling archive
    archive_folder = output_fs.makedirs("archive", recreate=True)
    archive = DataframeArchive(archive_folder, pipeline_config)

    # These are process folders tracking the different stages of the pipeline
    processing_folder = session_folder.makedirs("processing")
    cleaned_folder = processing_folder.makedirs("cleaned")
    degraded_folder = processing_folder.makedirs("degraded")
    enriched_folder = processing_folder.makedirs("enriched")
    error_container = ErrorContainer()

    # Current snapshot of the data - outside of session
    current_folder = output_fs.makedirs("current", recreate=True)

    # And these are files for export
    export_folder = output_fs.makedirs("export", recreate=True)

    for file_locator in locator_list:
        year = pl.discover_year(file_locator)
        filename = (
            file_locator.meta["name"]
            if "name" in file_locator.meta
            else file_locator.path
        )

        if year is None:
            error_container.append(
                dict(
                    filename=filename,
                    type="MissingYear",
                    message="Could not find a year in the filename or path",
                )
            )
        else:
            schema = load_schema(year)
            metadata = dict(year=year, schema=schema, la_code=la_code)

            cleanfile_result = task_cleanfile(file_locator, schema)

            cleanfile_result.data.export(
                cleaned_folder, file_locator.meta["uuid"] + "_", "parquet"
            )
            error_container.extend(cleanfile_result.errors)

            enrich_result = enrich_data(
                cleanfile_result.data, pipeline_config, metadata
            )
            enrich_result.export(
                enriched_folder, file_locator.meta["uuid"] + "_", "parquet"
            )

            degraded_result = degrade_data(enrich_result, pipeline_config, metadata)
            degraded_result.export(
                degraded_folder, file_locator.meta["uuid"] + "_", "parquet"
            )
            archive.add(degraded_result)

        # Write log specific file
        with session_folder.open(f"{file_locator.meta['uuid']}_log.txt", "wt") as FILE:
            error_container.filter("filename", filename).to_dataframe().to_csv(
                FILE, index=False
            )

    # Write complete log file
    with session_folder.open(f"session_log.txt", "wt") as FILE:
        error_container.to_dataframe().to_csv(FILE, index=False)

    current_data = archive.current()
    # Write archive
    current_data.export(current_folder, "ssda903_current_", "csv")

    for report in ["PAN", "SUFFICIENCY"]:
        report_data = prepare_export(current_data, pipeline_config, profile=report)
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data.export(report_folder, "ssda903_", "csv")
