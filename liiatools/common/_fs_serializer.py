"""
This is a temporary package to help us serialize/deserialize filesystems - it's not intended to be used outside of the pipeline and will change in future
"""
from fs.base import FS
from fs.osfs import OSFS
from fs.subfs import SubFS


def serialise(fs: FS) -> dict:
    path = ""
    if isinstance(fs, SubFS):
        path = fs._sub_dir
        fs = fs._wrap_fs

    if isinstance(fs, OSFS):
        return dict(type="osfs", path=fs.root_path, subpath=path)

    raise NotImplementedError(f"Cannot serialize {fs}")


def deserialise(data: dict) -> FS:
    if data["type"] == "osfs":
        fs = OSFS(data["path"])
        if data["subpath"]:
            fs = fs.opendir(data["subpath"])
        return fs

    raise NotImplementedError(f"Cannot deserialize {data}")
