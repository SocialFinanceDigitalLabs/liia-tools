import pickle
import uuid
from pathlib import Path

from fs import open_fs

from liiatools.common._fs_serializer import deserialise, serialise
from liiatools.common.data import FileLocator


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
    assert serialized["path"] == Path("build").resolve().as_posix()
    assert serialized["subpath"] == "/test/subfolder"


def test_deserialize():
    serialized = dict(
        type="osfs",
        path=Path("build").resolve().as_posix(),
        subpath="/test/subfolder",
    )
    deserialized = deserialise(serialized)

    assert deserialized._sub_dir == "/test/subfolder"
    assert deserialized._wrap_fs._root_path == Path("build").resolve().as_posix()


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
