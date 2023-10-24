import logging
import re
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig
from liiatools.common.spec.__data_schema import DataSchema

__ALL__ = ["load_schema", "DataSchema", "Category", "Column"]

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent


@lru_cache
def load_pipeline_config():
    with open(SCHEMA_DIR / "pipeline.yml", "rt") as FILE:
        return parse_yaml_file_as(PipelineConfig, FILE)


@lru_cache
def load_schema() -> DataSchema:
    schema_path = Path(SCHEMA_DIR, "Annex_A_schema.yml")

    # If we have no schema files, raise an error
    if not schema_path:
        raise ValueError(f"No schema files found")

    full_schema = yaml.safe_load(schema_path.read_text())

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema(**full_schema)
