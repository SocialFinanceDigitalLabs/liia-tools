from typing import List, Tuple

from dagster import In, Nothing, Out, op
from fs.base import FS
from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.data import DataContainer, FileLocator
from liiatools.common.transform import degrade_data, enrich_data, prepare_export
from liiatools.ssda903_pipeline.spec import load_schema
from liiatools.ssda903_pipeline.stream_pipeline import task_cleanfile

from liiatools_pipeline.assets.ssda903 import incoming_folder, pipeline_config, process_folder


@op(out={"session_folder": Out(FS), "incoming_files": Out(List[FileLocator])})
def create_session_folder() -> Tuple[FS, List[FileLocator]]:
    session_folder, incoming_files = pl.create_session_folder(
        incoming_folder(), process_folder()
    )
    return session_folder, incoming_files


@op(
    out={"archive": Out(DataframeArchive)},
)
def open_archive() -> DataframeArchive:
    archive_folder = process_folder().makedirs("archive", recreate=True)
    archive = DataframeArchive(archive_folder, pipeline_config())
    return archive


@op(
    ins={
        "session_folder": In(FS),
        "incoming_files": In(List[FileLocator]),
        "archive": In(DataframeArchive),
    },
)
def process_files(
    session_folder: FS,
    incoming_files: List[FileLocator],
    archive: DataframeArchive,
):
    for file_locator in incoming_files:
        year = pl.discover_year(file_locator)
        schema = load_schema(year)
        metadata = dict(year=year, schema=schema, la_code="BAD")

        cleanfile_result = task_cleanfile(file_locator, schema)

        processing_folder = session_folder.makedirs("processing")
        cleaned_folder = processing_folder.makedirs("cleaned")
        degraded_folder = processing_folder.makedirs("degraded")
        enriched_folder = processing_folder.makedirs("enriched")

        cleanfile_result.data.export(
            cleaned_folder, file_locator.meta["uuid"] + "_", "parquet"
        )
        # error_container.extend(cleanfile_result.errors)

        enrich_result = enrich_data(cleanfile_result.data, pipeline_config(), metadata)
        enrich_result.export(
            enriched_folder, file_locator.meta["uuid"] + "_", "parquet"
        )

        degraded_result = degrade_data(enrich_result, pipeline_config(), metadata)
        degraded_result.export(
            degraded_folder, file_locator.meta["uuid"] + "_", "parquet"
        )
        archive.add(degraded_result)


@op(
    ins={"archive": In(DataframeArchive), "start": In(Nothing)},
    out={"current_data": Out(DataContainer)},
)
def create_current_view(archive: DataframeArchive):
    current_folder = process_folder().makedirs("current", recreate=True)
    current_data = archive.current()

    # Write archive
    current_data.export(current_folder, "ssda903_", "csv")

    return current_data


@op(ins={"current_data": In(DataContainer)})
def create_reports(current_data: DataContainer):
    export_folder = process_folder().makedirs("export", recreate=True)

    for report in ["PAN", "SUFFICIENCY"]:
        report_folder = export_folder.makedirs(report, recreate=True)
        report = prepare_export(current_data, pipeline_config())
        report.export(report_folder, "ssda903_", "csv")
