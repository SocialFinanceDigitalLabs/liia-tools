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
        return dict(type="subfs", path=fs, subpath=path)

    if isinstance(fs, OSFS):
        return dict(type="osfs", path=fs.root_path, subpath=path)

    try:
        from fs_s3fs import S3FS

        if isinstance(fs, S3FS):
            return dict(type="s3", bucket=fs._bucket_name, subpath=path)
    except ImportError:
        pass

    raise NotImplementedError(f"Cannot serialize {fs}")


def deserialise(data: dict) -> FS:
    if data["type"] == "osfs":
        fs = OSFS(data["path"])
    elif data["type"] == "subfs":
        fs = SubFS(data["path"], data["subpath"])
    elif data["type"] == "s3":
        from fs_s3fs import S3FS

        fs = S3FS(data["bucket"])
    else:
        raise NotImplementedError(f"Cannot deserialize {data}")

    return fs


def pickle_FS(obj):
    # This example will only save the 'data' attribute
    # Modify this function to handle your actual serialization logic.
    serialized = serialise(obj)
    return unpickle_FS, (serialized,)


def unpickle_FS(data):
    # Modify this function to handle your actual deserialization logic.
    return deserialise(data)


def register():
    import copyreg

    copyreg.pickle(FS, pickle_FS)
    copyreg.pickle(SubFS, pickle_FS)
    copyreg.pickle(OSFS, pickle_FS)

    try:
        from fs_s3fs import S3FS

        copyreg.pickle(S3FS, pickle_FS)
    except ImportError:
        pass
