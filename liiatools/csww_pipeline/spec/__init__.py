from functools import lru_cache
from pathlib import Path

import xmlschema

from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig

SCHEMA_DIR = Path(__file__).parent


@lru_cache
def load_pipeline_config():
    with open(SCHEMA_DIR / "pipeline.yml", "rt") as FILE:
        return parse_yaml_file_as(PipelineConfig, FILE)


@lru_cache
def load_schema(year: int) -> xmlschema.XMLSchema:
    return xmlschema.XMLSchema(SCHEMA_DIR / f"social_work_workforce_schema_{year:04d}.xsd")


def load_schema_path(year: int) -> Path:
    return Path(SCHEMA_DIR, f"social_work_workforce_schema_{year:04d}.xsd")
