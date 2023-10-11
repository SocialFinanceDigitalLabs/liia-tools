import hashlib
import logging
import uuid
from datetime import datetime
from os.path import basename, dirname
from typing import Iterable, List, Tuple

import pandas as pd
import yaml
from fs.base import FS
from fs.info import Info
from fs.move import move_file, copy_file

from liiatools.common.constants import ProcessNames, SessionNames
from liiatools.datasets.shared_functions.common import check_year

from .data import FileLocator

logger = logging.getLogger(__name__)


def _get_timestamp():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S.%f")


def _move_incoming_file(
    source_fs: FS, dest_fs: FS, file_path: str, file_info: Info
) -> FileLocator:
    """
    Move a file from the incoming folder to the correct location in the archive.
    """
    file_uuid = uuid.uuid4().hex
    file_sha = hashlib.sha256(source_fs.readbytes(file_path)).hexdigest()

    file_locator = FileLocator(
        dest_fs,
        file_uuid,
        original_path=file_path,
        metadata={
            "path": file_path,
            "name": file_info.name,
            "size": file_info.size,
            "modified": file_info.modified,
            "uuid": file_uuid,
            "sha256": file_sha,
        },
    )

    dest_fs.writetext(f"{file_uuid}_meta.yaml", yaml.dump(file_locator.meta))
    copy_file(source_fs, file_path, dest_fs, file_uuid)

    return file_locator


def create_process_folders(destionation_fs: FS):
    """
    Ensures that all the standard process folders are created
    """
    for folder in ProcessNames:
        destionation_fs.makedirs(folder, recreate=True)


def create_session_folder(destionation_fs: FS) -> Tuple[FS, str]:
    """
    Create a new session folder in the output filesystem with the standard folders
    """
    session_id = _get_timestamp()
    session_folder = destionation_fs.makedirs(
        f"{ProcessNames.SESSIONS_FOLDER}/{session_id}"
    )
    for folder in SessionNames:
        session_folder.makedirs(folder)
    return session_folder, session_id


def move_files_for_processing(
    source_fs: FS, session_fs: FS, continue_on_error: bool = False
) -> Iterable[FileLocator]:
    """
    Moves all files from the source filesystem to the session folder. This is a generator function that yields a FileLocator for each file moved.
    """

    destination_fs = session_fs.opendir(SessionNames.INCOMING_FOLDER)
    source_file_list = source_fs.walk.info(namespaces=["details"])

    for file_path, file_info in source_file_list:
        if file_info.is_file:
            try:
                yield _move_incoming_file(
                    source_fs, destination_fs, file_path, file_info
                )
            except Exception as e:
                logger.error(f"Error moving file {file_path} to session folder")
                if continue_on_error:
                    pass
                else:
                    raise e


def restore_session_folder(session_fs: FS) -> List[FileLocator]:
    """
    Should we ever need to re-run a pipeline, this function will restore the session folder and return the metadata just like the create_session_folder function.
    """
    incoming_fs = session_fs.opendir(SessionNames.INCOMING_FOLDER)
    files = incoming_fs.listdir(".")
    metadata_files = [f"{f}" for f in files if f.endswith("_meta.yaml")]

    file_locators = []
    for filename in metadata_files:
        md = yaml.safe_load(incoming_fs.readtext(filename))
        file_locators.append(
            FileLocator(incoming_fs, md["uuid"], original_path=md["path"], metadata=md)
        )

    return file_locators


def discover_year(file_locator: FileLocator) -> int:
    """
    Try to discover the year for a file.

    This function will try to find a year in the path, and if that fails, it will try to find a year in the full filename.

    If the year is found, it will be added to the file metadata.
    """
    file_dir = dirname(file_locator.name)
    file_name = basename(file_locator.name)

    # Check year doesn't currently return an int
    def _check_year(s: str) -> int:
        return int(check_year(s))

    try:
        return _check_year(file_dir)
    except ValueError:
        pass

    try:
        return _check_year(file_name)
    except ValueError:
        pass
