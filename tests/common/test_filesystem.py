import pickle
import uuid
from pathlib import Path

import pytest
from fs import open_fs

from liiatools.common._fs_serializer import deserialise, register, serialise
from liiatools.common.data import FileLocator

register()


def test_filesystem():
    myfs = open_fs("build")

    test_folder = myfs.makedirs("test", recreate=True)
    subfolder = test_folder.makedirs("subfolder", recreate=True)

    assert subfolder._sub_dir == "/test/subfolder"
    assert subfolder._wrap_fs == myfs


def test_serialize():
    myfs = open_fs("build")

    test_folder = myfs.makedirs("test", recreate=True)
    subfolder = test_folder.makedirs("subfolder", recreate=True)

    serialized = serialise(subfolder)

    assert serialized["type"] == "osfs"
    assert Path(serialized["path"]) == Path("build").resolve()
    assert serialized["subpath"] == "/test/subfolder"


def test_deserialize():
    serialized = dict(
        type="osfs",
        path=Path("build").resolve().as_posix(),
        subpath="/test/subfolder",
    )
    deserialized = deserialise(serialized)

    assert deserialized._sub_dir == "/test/subfolder"
    assert Path(deserialized._wrap_fs._root_path) == Path("build").resolve()


def test_pickle():
    myfs = open_fs("build")

    test_folder = myfs.makedirs("test", recreate=True)
    subfolder = test_folder.makedirs("subfolder", recreate=True)

    locator = FileLocator(subfolder, "test.txt")

    pickled = pickle.dumps(locator)
    unpickled = pickle.loads(pickled)

    unique_string = uuid.uuid4().hex
    subfolder.writetext("test.txt", unique_string)

    with unpickled.open("rt") as FILE:
        assert FILE.read() == unique_string


def test_fs_pickle():
    myfs = open_fs("build")
    test_folder = myfs.makedirs("test", recreate=True)
    subfolder = test_folder.makedirs("subfolder", recreate=True)

    pickled = pickle.dumps(subfolder)
    unpickled = pickle.loads(pickled)

    assert unpickled


def test_s3_pickle():
    pytest.importorskip("fs_s3fs")

    myfs = open_fs("s3://mybucket")

    pickled = pickle.dumps(myfs)
    unpickled = pickle.loads(pickled)

    assert unpickled
    assert type(unpickled).__name__ == "S3FS"
