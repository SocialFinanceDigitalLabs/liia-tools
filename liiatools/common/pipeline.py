import hashlib
import uuid
from datetime import datetime
from os.path import dirname
from typing import Dict, List, Tuple

import pandas as pd
import yaml
from fs.base import FS
from fs.info import Info
from fs.move import move_file

from liiatools.datasets.shared_functions.common import check_year

from .data import FileLocator


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

    metadata = FileLocator(
        dest_fs,
        file_uuid,
        {
            "path": file_path,
            "name": file_info.name,
            "size": file_info.size,
            "modified": file_info.modified,
            "uuid": file_uuid,
            "sha256": file_sha,
        },
    )

    dest_fs.writetext(f"{file_uuid}_meta.yaml", yaml.dump(metadata.meta))
    move_file(source_fs, file_path, dest_fs, file_uuid)

    return metadata


SESSION_FOLDER_PREFIX = "sessions"
SESSION_INCOMING_PREFIX = "incoming"


def create_session_folder(
    source_fs: FS, destionation_fs: FS
) -> Tuple[FS, List[FileLocator]]:
    """
    Create a new session folder in the output filesystem, and move all the source files into it.
    """
    session_id = _get_timestamp()
    session_fs = destionation_fs.makedirs(f"{SESSION_FOLDER_PREFIX}/{session_id}")
    destination_fs = session_fs.makedirs(SESSION_INCOMING_PREFIX)

    source_file_list = source_fs.walk.info(namespaces=["details"])

    file_locators = []
    for file_path, file_info in source_file_list:
        if file_info.is_file:
            md = _move_incoming_file(source_fs, destination_fs, file_path, file_info)
            file_locators.append(md)

    return session_fs, file_locators


def restore_session_folder(session_fs: FS) -> List[FileLocator]:
    """
    Should we ever need to re-run a pipeline, this function will restore the session folder and return the metadata just like the create_session_folder function.
    """
    incoming_fs = session_fs.opendir(SESSION_INCOMING_PREFIX)
    files = incoming_fs.listdir(".")
    metadata_files = [f"{f}" for f in files if f.endswith("_meta.yaml")]

    file_locators = []
    for filename in metadata_files:
        md = yaml.safe_load(incoming_fs.readtext(filename))
        file_locators.append(FileLocator(incoming_fs, md["uuid"], md))

    return file_locators


def discover_year(file_locator: FileLocator) -> int:
    """
    Try to discover the year for a file.

    This function will try to find a year in the path, and if that fails, it will try to find a year in the full filename.

    If the year is found, it will be added to the file metadata.
    """
    file_path = file_locator.meta["path"]
    file_dir = dirname(file_path)
    file_name = file_locator.meta["name"]

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
