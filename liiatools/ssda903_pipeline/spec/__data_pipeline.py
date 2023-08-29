from typing import Any, Dict, Iterable, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class ColumnConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: Literal["integer", "category", "date", "string", "postcode"]
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


def build_data_from_old_config():
    from pathlib import Path

    import yaml

    from liiatools.ssda903_pipeline.spec import load_schema

    CFG_FOLDER = Path(__file__).parent

    schema = load_schema(2050)

    tables = []

    for table_name, table_columns in schema.table.items():
        table_schema = schema.table[table_name]
        table_config = TableConfig(
            id=table_name,
            columns=[
                ColumnConfig(id=column_name, type=table_schema[column_name].type)
                for column_name in list(table_columns.keys())
            ]
            + [
                ColumnConfig(id="LA", type="string", enrich="la_code"),
                ColumnConfig(id="YEAR", type="integer", enrich="year"),
            ],
        )
        table_config["CHILD"].enrich = "add_la_suffix"

        for column_config in table_config.columns:
            if column_config.type == "postcode":
                column_config.degrade = "short_postcode"
            if column_config.id in ["DOB", "MC_DOB"]:
                column_config.degrade = "first_of_month"

        tables.append(table_config)

    tables = PipelineConfig(schema=tables)

    # Import LA-AGG configuration:
    with open(CFG_FOLDER / "la-agg.yml", "rt") as FILE:
        la_agg = yaml.safe_load(FILE)

    for table_name, column_list in la_agg["sort_order"].items():
        for ix, sort_column in enumerate(column_list):
            tables[table_name][sort_column].sort = ix

    for table_name, column_list in la_agg["dedup"].items():
        for ix, sort_column in enumerate(column_list):
            tables[table_name][sort_column].unique_key = True

    # Import PAN-AGG configuration:
    with open(CFG_FOLDER / "pan-agg.yml", "rt") as FILE:
        pan_agg = yaml.safe_load(FILE)

    for table_name in pan_agg["pan_data_kept"]:
        tables[table_name].retain.append("PAN")

    # Import SUFFICIENCY configuration:
    with open(CFG_FOLDER / "sufficiency.yml", "rt") as FILE:
        sufficiency = yaml.safe_load(FILE)

    for table_name in sufficiency["suff_data_kept"]:
        tables[table_name].retain.append("SUFFICIENCY")

    for table_name, column_list in sufficiency["minimise"].items():
        for column_name in column_list:
            tables[table_name][column_name].exclude.append("SUFFICIENCY")

    with open(CFG_FOLDER / "pipeline.yml", "wt") as FILE:
        yaml.safe_dump(
            tables.model_dump(exclude_none=True, exclude_defaults=True),
            FILE,
            sort_keys=False,
        )
