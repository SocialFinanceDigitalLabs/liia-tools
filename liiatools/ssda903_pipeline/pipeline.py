import logging

from typing import List, Tuple
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.aggregate import DataframeAggregator
from liiatools.common.constants import SessionNames
from liiatools.common.data import (
    ErrorContainer,
    FileLocator,
)
from liiatools.common.reference import authorities
from liiatools.common.transform import degrade_data, enrich_data, prepare_export

from liiatools.ssda903_pipeline.spec import load_pipeline_config, load_schema
from liiatools.ssda903_pipeline.stream_pipeline import task_cleanfile

logger = logging.getLogger()


def create_session_folder(process_folder: FS, incoming_folder: FS) -> Tuple[FS, str, List[FileLocator]]:
    session_folder, session_id = pl.create_session_folder(process_folder)
    incoming_files = pl.move_files_for_processing(incoming_folder, session_folder)

    return session_folder, session_id, incoming_files


def open_archive(session_id: str, process_folder: FS) -> DataframeArchive:
    archive_folder = process_folder.makedirs("archive", recreate=True)
    archive = DataframeArchive(archive_folder, load_pipeline_config(), session_id)
    return archive


def process_files(
        session_folder: FS,
        incoming_files: List[FileLocator],
        archive: DataframeArchive,
        session_id: str,
        input_la_code: str = None,
):
    error_report = ErrorContainer()
    for file_locator in incoming_files:
        uuid = file_locator.meta["uuid"]
        year = pl.discover_year(file_locator)
        if year is None:
            error_report.append(
                dict(
                    type="MissingYear",
                    message="Could not find a year in the filename or path",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        la_code = input_la_code if input_la_code is not None else pl.discover_la(file_locator)
        if la_code is None:
            error_report.append(
                dict(
                    type="MissingLA",
                    message="Could not find a local authority in the filename or path",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        schema = load_schema(year)
        metadata = dict(year=year, schema=schema, la_code=la_code)

        try:
            cleanfile_result = task_cleanfile(file_locator, schema)
        except Exception as e:
            error_report.append(
                dict(
                    type="StreamError",
                    message="Failed to clean file. Check log files for technical errors.",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        cleanfile_result.data.export(
            session_folder.opendir(SessionNames.CLEANED_FOLDER), file_locator.meta["uuid"] + "_", "parquet"
        )
        error_report.extend(cleanfile_result.errors)

        enrich_result = enrich_data(cleanfile_result.data, load_pipeline_config(), metadata)
        enrich_result.data.export(
            session_folder.opendir(SessionNames.ENRICHED_FOLDER), file_locator.meta["uuid"] + "_", "parquet"
        )
        error_report.extend(enrich_result.errors)

        degraded_result = degrade_data(enrich_result.data, load_pipeline_config(), metadata)
        degraded_result.data.export(
            session_folder.opendir(SessionNames.DEGRADED_FOLDER), file_locator.meta["uuid"] + "_", "parquet"
        )
        error_report.extend(degraded_result.errors)
        archive.add(degraded_result.data, la_code)

        error_report.set_property("filename", file_locator.name)
        error_report.set_property("uuid", uuid)

    error_report.set_property("session_id", session_id)
    with session_folder.open("error_report.csv", "w") as FILE:
        error_report.to_dataframe().to_csv(FILE, index=False)


def create_current_view(archive: DataframeArchive, process_folder: FS) -> FS:
    archive.rollup()
    current_folder = process_folder.makedirs("current", recreate=True)
    for la_code in authorities.codes:
        current_data = archive.current(la_code)

        if current_data:
            la_folder = current_folder.makedirs(la_code, recreate=True)
            current_data.export(la_folder, "ssda903_", "csv")

    return current_folder


def create_reports(current_folder: FS, process_folder: FS):
    export_folder = process_folder.makedirs("export", recreate=True)
    aggregate = DataframeAggregator(current_folder, load_pipeline_config())
    aggregate_data = aggregate.current()

    for report in ["PAN", "SUFFICIENCY"]:
        report_folder = export_folder.makedirs(report, recreate=True)
        report = prepare_export(aggregate_data, load_pipeline_config())
        report.data.export(report_folder, "ssda903_", "csv")
