from typing import List

from pydantic import BaseModel, ConfigDict


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

    def __getitem__(self, value) -> ColumnConfig:
        ix = {t.id: t for t in self.columns}
        return ix[value]

    @property
    def sort_keys(self) -> List[str]:
        """
        Returns a list of column ids that should be used to sort the table in order of priority
        """
        sort_keys = [(c.id, c.sort) for c in self.columns if c.sort]
        sort_keys.sort(key=lambda x: x[1])
        return [c[0] for c in sort_keys]

    def columns_for_profile(self, profile: str) -> List[ColumnConfig]:
        return [c for c in self.columns if profile not in c.exclude]


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_list: List[TableConfig]

    def __getitem__(self, value) -> TableConfig:
        ix = {t.id: t for t in self.table_list}
        return ix[value]

    def tables_for_profile(self, profile: str) -> List[TableConfig]:
        return [t for t in self.table_list if profile in t.retain]
