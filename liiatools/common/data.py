from contextlib import contextmanager
from typing import Any, Dict, List

import pandas as pd
from fs.base import FS
from pydantic import BaseModel, ConfigDict
from tablib import Databook, Dataset, import_set
from tablib.formats import registry as tablib_registry

DataContainer = Dict[str, pd.DataFrame]
Metadata = Dict[str, Any]


class FileLocator:
    def __init__(self, fs: FS, path: str, metadata: Metadata = None):
        self.__fs = fs
        self.__path = path

        self.__metadata = metadata or {}

    @property
    def path(self) -> str:
        return self.__path

    @property
    def meta(self) -> Metadata:
        return self.__metadata

    def open(self, mode: str = "r"):
        return self.__fs.open(self.__path, mode)


class DataContainer(Dict[str, pd.DataFrame]):
    """
    DataContainer is a dictionary of DataFrames, with some helper methods to convert to tablib objects and export to filesystems.

    The object can be passed around between pipeline jobs.
    """

    def to_dataset(self, key: str) -> Dataset:
        dataset = import_set(self[key], "df")
        dataset.title = key
        return dataset

    def to_databook(self) -> Databook:
        return Databook([self.to_dataset(k) for k in self.keys()])

    def copy(self) -> DataContainer:
        """
        Returns a deep copy of the DataContainer
        """
        return DataContainer({k: v.copy() for k, v in self.items()})

    def export(self, fs: FS, basename: str, format="csv"):
        """
        Export the data to a filesystem. Supports any format supported by tablib, plus parquet.

        If the format supports multiple sheets (e.g. xlsx), then each table will be exported to a separate sheet in the same file,
        otherwise each table will be exported to a separate file.
        """
        if format == "parquet":
            return self._export_parquet(fs, basename)

        fmt = tablib_registry.get_format(format)
        fmt_ext = fmt.extensions[0]

        if hasattr(fmt, "export_book"):
            book = self.to_databook()
            data = book.export(format)
            self._write(fs, f"{basename}.{fmt_ext}", data)
        else:
            for table_name in self:
                dataset = self.to_dataset(table_name)
                data = dataset.export(format)
                self._write(fs, f"{basename}{table_name}.{fmt_ext}", data)

    def _export_parquet(self, fs: FS, basename: str):
        for table_name in self:
            df = self[table_name]
            with fs.open(f"{basename}{table_name}.parquet", "wb") as f:
                df.to_parquet(f, index=False)

    def _write(self, fs: FS, path: str, data: Any):
        format = "wt" if isinstance(data, str) else "wb"
        with fs.open(path, format) as f:
            f.write(data)


class ErrorContainer(List[Dict[str, Any]]):
    """
    Used for holding data quality errors during processing. Can be used to filter the list of errors by a property so can limit to specific context, e.g. for a particular file.
    """

    def filter(self, prop: str, value: Any) -> "ErrorContainer":
        return ErrorContainer([e for e in self if e.get(prop) == value])

    def with_prop(self, prop: str) -> "ErrorContainer":
        return ErrorContainer([e for e in self if prop in e])

    def to_dataframe(self):
        return pd.DataFrame(self)


class ColumnConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    unique_key: bool = False
    enrich: str = None
    degrade: str = None
    sort: int = None
    exclude: List[str] = []


class TableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    retain: List[str] = []
    columns: List[ColumnConfig]

    def __getitem__(self, value):
        ix = {t.id: t for t in self.columns}
        return ix[value]

    @property
    def sort_keys(self):
        sort_keys = [(c.id, c.sort) for c in self.columns if c.sort]
        sort_keys.sort(key=lambda x: x[1])
        return [c[0] for c in sort_keys]

    def columns_for_profile(self, profile: str):
        return [c for c in self.columns if profile not in c.exclude]


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_list: List[TableConfig]

    def __getitem__(self, value):
        ix = {t.id: t for t in self.table_list}
        return ix[value]

    def tables_for_profile(self, profile: str):
        return [t for t in self.table_list if profile in t.retain]
