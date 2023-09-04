from typing import Any, Dict

from fs.base import FS

Metadata = Dict[str, Any]


class FileLocator:
    """
    A FileLocator is a pointer to a file in a virtual filesystem. It contains the information required to open the file, as well as some
    metadata about the file.

    One import feature of the file locator is that the storage location can have an opaque name whilst the location can still retain the
    key properties of the origina file. What does this mean? When we process files, we move files around and we want to make sure names don't clash,
    so we actually give each file a unique code that is not related to the original filename. However, we still want to be able to track the original
    filename, so we store it in the metadata.

    The file locator allows us to do handle this complexity. The `name` property returns the original path, if given, otherwise the file location.

    :param fs: The filesystem containing the file
    :param file_location: The location of the file in the filesystem
    :param original_path: The original path of the file, if different from the file_location.
    :param metadata: Any metadata about the file.


    """

    def __init__(
        self,
        fs: FS,
        file_location: str,
        *,
        original_path: str = None,
        metadata: Metadata = None,
    ):
        self.__fs = fs
        self.__path = file_location
        self.__public_path = original_path or file_location
        self.__metadata = metadata or {}

    @property
    def name(self) -> str:
        return self.__public_path

    @property
    def meta(self) -> Metadata:
        return self.__metadata

    def open(self, mode: str = "r"):
        return self.__fs.open(self.__path, mode)

    def __repr__(self):
        if self.__public_path != self.__path:
            return f"FileLocator({self.__fs}, {self.__public_path} [{self.__path}])"
        return f"FileLocator({self.__fs}, {self.__path})"
