from dagster import job

from liiatools_pipeline.ops import ssda903


@job
def ssda903_incoming():
    session_folder, incoming_files = ssda903.create_session_folder()
    archive = ssda903.open_archive()

    processed = ssda903.process_files(session_folder, incoming_files, archive)

    current_data = ssda903.create_current_view(archive, start=processed)

    ssda903.create_reports(current_data)
