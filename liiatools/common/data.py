from typing import Any, Dict, List

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

DataContainer = Dict[str, pd.DataFrame]
Metadata = Dict[str, Any]


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
