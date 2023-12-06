from datetime import datetime
import logging

import pandas as pd
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.constants import ProcessNames, SessionNames
from liiatools.common.data import (
    DataContainer,
    ErrorContainer,
    FileLocator,
    PipelineConfig,
    ProcessResult,
)
from liiatools.common.transform import degrade_data, enrich_data, prepare_export

from liiatools.csww_pipeline.spec import (
    load_schema,
    load_schema_path,
    load_pipeline_config,
)
from liiatools.csww_pipeline.stream_pipeline import task_cleanfile

# dependencies for met_analysis()
from liiatools.csww_pipeline.met_analysis import (
    growth_tables,
    pivotGen,
    seniority,
    FTESum,
    validator as met_validator,
)

logger = logging.getLogger()


def met_analysis(csww_df: pd.DataFrame, public_fs: FileLocator) -> DataContainer:
    """
    Run the MET analysis
    :param csww_df: Children's Social Work Workforce data
    :param public_fs: The pointer to the public data virtual filesystem
    :return: DataContainer with demographic breakdown and forecast information
    """
    year = datetime.now().year

    # Load growth tables
    population_growth_table = growth_tables.growth_tables(public_fs, year=year)

    # Validate data
    csww_df = met_validator.remove_invalid_worker_data(
        csww_df, met_validator.NON_AGENCY_MANDATORY_TAG
    )

    # Create demographic table
    csww_df = seniority.add_seniority_and_retention_columns(csww_df)
    demographic_table = pivotGen.create_demographic_table(csww_df)

    # Create forecast
    csww_df = seniority.convert_codes_to_names(csww_df)
    fte_sum = FTESum.FTESum(csww_df, year=year)
    seniority_forecast = seniority.seniority_forecast(fte_sum, population_growth_table)

    data = DataContainer(
        {"demographics": demographic_table, "forecast": seniority_forecast}
    )

    return data


def process_file(
    file_locator: FileLocator,
    session_folder: FS,
    pipeline_config: PipelineConfig,
    la_code: str,
) -> ProcessResult:
    """
    Clean, enrich and degrade data
    :param file_locator: The pointer to a file in a virtual filesystem
    :param session_folder: The path to the session folder
    :param pipeline_config: The pipeline configuration
    :param la_code: A three-letter string for the local authority depositing the file
    :return: A class containing a DataContainer and ErrorContainer
    """
    errors = ErrorContainer()
    year = pl.discover_year(file_locator)
    if year is None:
        errors.append(
            dict(
                type="MissingYear",
                message="Could not find a year in the filename or path",
                filename=file_locator.name,
            )
        )
        return ProcessResult(data=None, errors=errors)

    # We save these files based on the session UUID - so UUID must exist
    uuid = file_locator.meta["uuid"]

    # Load schema and set on processing metadata
    schema = load_schema(year=year)
    schema_path = load_schema_path(year=year)
    metadata = dict(year=year, schema=schema, la_code=la_code)

    # Normalise the data and export to the session 'cleaned' folder
    try:
        cleanfile_result = task_cleanfile(file_locator, schema, schema_path)
    except Exception as e:
        logger.exception(f"Error cleaning file {file_locator.name}")
        errors.append(
            dict(
                type="StreamError",
                message="Failed to clean file. Check log files for technical errors.",
                filename=file_locator.name,
            )
        )
        return ProcessResult(data=None, errors=errors)

    # Export the cleaned data to the session 'cleaned' folder
    cleanfile_result.data.export(
        session_folder, f"{SessionNames.CLEANED_FOLDER}/{uuid}_", "parquet"
    )
    errors.extend(cleanfile_result.errors)

    # Enrich the data and export to the session 'enriched' folder
    enrich_result = enrich_data(cleanfile_result.data, pipeline_config, metadata)
    enrich_result.data.export(
        session_folder, f"{SessionNames.ENRICHED_FOLDER}/{uuid}_", "parquet"
    )
    errors.extend(enrich_result.errors)

    # Degrade the data and export to the session 'degraded' folder
    degraded_result = degrade_data(enrich_result.data, pipeline_config, metadata)
    degraded_result.data.export(
        session_folder, f"{SessionNames.DEGRADED_FOLDER}/{uuid}_", "parquet"
    )
    errors.extend(degraded_result.errors)

    errors.set_property("filename", file_locator.name)
    errors.set_property("uuid", uuid)

    return ProcessResult(data=degraded_result.data, errors=errors)


def process_session(source_fs: FS, output_fs: FS, la_code: str, public_fs: FileLocator):
    """
    Runs the full pipeline on a file or folder
    :param source_fs: File system containing the input files
    :param output_fs: File system for the output files
    :param la_code: A three-letter string for the local authority depositing the file
    :param public_fs: The FileLocator to public datasets needed for analysis
    :return: None
    """
    # Before we start - load configuration for this dataset
    pipeline_config = load_pipeline_config()

    # Ensure all processing folders exist
    pl.create_process_folders(output_fs)

    # Create session folder
    session_folder, session_id = pl.create_session_folder(output_fs)

    # Move files into session folder
    locator_list = pl.move_files_for_processing(source_fs, session_folder)

    # Process each incoming file
    processed_files = [
        process_file(locator, session_folder, pipeline_config, la_code)
        for locator in locator_list
    ]

    # Add processed files to archive
    archive = DataframeArchive(
        output_fs.opendir(ProcessNames.ARCHIVE_FOLDER), pipeline_config
    )
    for result in processed_files:
        if result.data:
            archive.add(result.data)

    # Write the error summary
    error_summary = ErrorContainer(
        [error for result in processed_files for error in result.errors]
    )
    error_summary.set_property("session_id", session_id)
    with session_folder.open("error_summary.csv", "w") as FILE:
        error_summary.to_dataframe().to_csv(FILE, index=False)

    # Export the current snapshot of the archive
    current_data = archive.current()
    current_data.export(
        output_fs.opendir(ProcessNames.CURRENT_FOLDER), "csww_current_", "csv"
    )

    # Create the different reports
    export_folder = output_fs.opendir(ProcessNames.EXPORT_FOLDER)
    for report in ["PAN"]:
        report_data = prepare_export(current_data, pipeline_config, profile=report)
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data.data.export(report_folder, "csww_", "csv")

    # Run MET analysis
    met_data = met_analysis(report_data.data["Worker"], public_fs)
    met_folder = export_folder.makedirs("MET", recreate=True)
    met_data.export(met_folder, "csww_", "csv")
