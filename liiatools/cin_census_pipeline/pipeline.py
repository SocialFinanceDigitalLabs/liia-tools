from fs import FS

from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.constants import ProcessNames, SessionNames
from liiatools.common.data import (
    DataContainer,
    ErrorContainer,
    FileLocator,
    PipelineConfig,
    ProcessResult,
    TableConfig,
)
from liiatools.common.transform import degrade_data, enrich_data, prepare_export


def process_file(
    file_locator: FileLocator,
    session_folder: FS,
    pipeline_config: PipelineConfig,
    la_code: str,
) -> ProcessResult:
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

    metadata = dict(year=year, schema=schema, la_code=la_code)
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


def process_session(source_fs: FS, output_fs: FS, la_code: str):
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
        output_fs.opendir(ProcessNames.CURRENT_FOLDER), "cin_cencus_current_", "csv"
    )
