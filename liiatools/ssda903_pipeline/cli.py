import logging
from datetime import datetime
from pathlib import Path

import click as click
import click_log
import tablib

from liiatools.common.transform import degrade_data, enrich_data, prepare_export
from liiatools.datasets.shared_functions.common import (
    check_file_type,
    check_year,
    check_year_within_range,
    save_incorrect_year_error,
    save_year_error,
    supported_file_types,
)
from liiatools.spec.common import authorities

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
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--la_code",
    required=True,
    type=click.Choice(authorities.codes, case_sensitive=False),
    help="A three letter code, specifying the local authority that deposited the file",
)
@click.option(
    "--la_log_dir",
    required=True,
    type=str,
    help="A string specifying the location that the log files for the LA should be output, usable by a pathlib Path function.",
)
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output directory location",
)
@click_log.simple_verbosity_option(log)
def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input SSDA903 csv files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Prepare file
    # List of commonly submitted unneeded files
    # drop_file_list = [
    #     "Extended Review",
    #     "Pupil Premium Children",
    #     "Children Ceasing to be looked after for other reasons",
    #     "Distance and Placement Extended",
    #     "Extended Adoption",
    #     "Children Ceased Care During the Year",
    #     "Children Looked After on 31st March",
    #     "Children Started Care During the Year",
    # ]
    # prep.delete_unrequired_files(
    #     input, drop_file_list=drop_file_list, la_log_dir=la_log_dir
    # )
    # if prep.check_blank_file(input, la_log_dir=la_log_dir) == "empty":
    #     return
    # prep.drop_empty_rows(input, input)

    # Configuration
    try:
        filename = str(Path(input).resolve().stem)
        year = check_year(filename)
        year = int(year)
    except (AttributeError, ValueError):
        save_year_error(input, la_log_dir)
        return

    if (
        check_year_within_range(
            year, YEARS_TO_GO_BACK, YEAR_START_MONTH, REFERENCE_DATE
        )
        is False
    ):
        save_incorrect_year_error(input, la_log_dir)
        return

    if (
        check_file_type(
            input,
            file_types=[".csv"],
            supported_file_types=supported_file_types,
            la_log_dir=la_log_dir,
        )
        == "incorrect file type"
    ):
        return

    schema = load_schema(year)
    metadata = dict(year=year, schema=schema, la_code=la_code)

    with open(input, "rt") as f:
        input_data = tablib.import_set(f, "csv")

    cleanfile_result = task_cleanfile(input_data, schema)

    for table_name, table_data in cleanfile_result.data.items():
        table_data.to_csv(f"{output}/{table_name}.csv", index=False)

    # TODO: Write data quality report

    pipeline_config = load_pipeline_config()

    enrich_result = enrich_data(cleanfile_result.data, pipeline_config, metadata)

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
