import logging
from datetime import datetime

from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.constants import ProcessNames, SessionNames
from liiatools.common.data import (
    ErrorContainer,
    FileLocator,
    PipelineConfig,
    ProcessResult,
)
from liiatools.common.transform import degrade_data, enrich_data, prepare_export

from .spec import load_pipeline_config, load_schema
from .stream_pipeline import task_cleanfile

logger = logging.getLogger()


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
    year, financial_year, quarter, error = pl.find_year_from_column(
        file_locator,
        columns=["Placement end date", "End date"],
        retention_period=7,
        reference_year=datetime.now().year
    )
    if error is not None:
        if error == pl.DataType.MISSING_COLUMN:
            error_type = "MissingYear"
            message = "Could not find column which is used to identify year of return"
        elif error == pl.DataType.EMPTY_COLUMN:
            error_type = "MissingYear"
            message = "Column used to identify year of return was empty"
        elif error == pl.DataType.OLD_DATA:
            error_type = "OldYear"
            message = f"File is from {financial_year} and we are only accepting data from 2023 onwards"
        elif error == pl.DataType.ENCODING_ERROR:
            error_type = "EncodingError"
            message = f"File was saved in the wrong format. The correct format is CSV UTF-8. To fix this open the " \
                      f"file in Excel, click 'Save As' and select 'CSV UTF-8 (Comma delimited) (*csv)'"

        errors.append(
            dict(
                type=error_type,
                message=message,
                filename=file_locator.name,
            )
        )
        return ProcessResult(data=None, errors=errors)

    # We save these files based on the session UUID - so UUID must exist
    uuid = file_locator.meta["uuid"]

    # Load schema and set on processing metadata
    schema = load_schema(financial_year)
    metadata = dict(year=year, quarter=quarter, schema=schema, la_code=la_code)

    # Normalise the data and export to the session 'cleaned' folder
    try:
        cleanfile_result = task_cleanfile(file_locator, schema)
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


def process_session(source_fs: FS, output_fs: FS, la_code: str):
    """
    Runs the full pipeline on a file or folder
    :param source_fs: File system containing the input files
    :param output_fs: File system for the output files
    :param la_code: A three-letter string for the local authority depositing the file
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
        output_fs.opendir(ProcessNames.CURRENT_FOLDER), "s251_current_", "csv"
    )

    # Create the different reports
    export_folder = output_fs.opendir(ProcessNames.EXPORT_FOLDER)
    for report in ["PAN"]:
        report_data = prepare_export(current_data, pipeline_config, profile=report)
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data.data.export(report_folder, "s251_", "csv")