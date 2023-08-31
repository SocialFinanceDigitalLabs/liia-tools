import logging
from datetime import datetime
from pathlib import Path

import click as click
import click_log
import pandas as pd
import tablib
from fs import open_fs

from liiatools.common import pipeline as pl
from liiatools.common.reference import authorities
from liiatools.common.transform import degrade_data, enrich_data, prepare_export
from liiatools.datasets.shared_functions.common import (
    check_file_type,
    check_year,
    check_year_within_range,
    save_incorrect_year_error,
    save_year_error,
    supported_file_types,
)

from .spec import load_pipeline_config, load_schema
from .stream_pipeline import task_cleanfile

log = logging.getLogger()
click_log.basic_config(log)

YEARS_TO_GO_BACK = 7
YEAR_START_MONTH = 1
REFERENCE_DATE = datetime.now()


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

    cleaned_folder = session_folder.makedirs("cleaned")

    for file_locator in locator_list:
        year = pl.discover_year(file_locator)

        if year is None:
            # Write an error report entry
            pass
        else:
            schema = load_schema(year)
            metadata = dict(year=year, schema=schema, la_code=la_code)

            cleanfile_result = task_cleanfile(file_locator, schema)

            cleanfile_result.data.export(
                cleaned_folder, file_locator.meta["uuid"], "parquet"
            )

            # # TODO: Write data quality report
            # error_report = pd.DataFrame(cleanfile_result.errors)
            # error_report = error_report[
            #     ["table_name", "header", "r_ix", "c_ix", "type", "message"]
            # ]
            # error_report.columns = ["Table", "Header", "Row", "Column", "Type", "Message"]
            # error_report.to_csv(f"{output}/error_report.csv", index=False)

            enrich_result = enrich_data(
                cleanfile_result.data, pipeline_config, metadata
            )

            for table_name, table_data in enrich_result.items():
                table_data.to_csv(f"{output}/enriched_{table_name}.csv", index=False)

            degraded_result = degrade_data(enrich_result, pipeline_config, metadata)

    # TODO: Create historic archive here

    for table_name, table_data in degraded_result.items():
        table_data.to_csv(f"{output}/degraded_{table_name}.csv", index=False)

    for report in ["PAN", "SUFFICIENCY"]:
        report_data = prepare_export(degraded_result, pipeline_config, profile=report)
        for table_name, table_data in report_data.items():
            table_data.to_csv(f"{output}/report_{report}_{table_name}.csv", index=False)
