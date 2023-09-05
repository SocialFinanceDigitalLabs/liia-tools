from functools import lru_cache
from pathlib import Path

import xmlschema

SCHEMA_DIR = Path(__file__).parent


@lru_cache
def load_schema(year: int) -> xmlschema.XMLSchema:
    return xmlschema.XMLSchema(SCHEMA_DIR / f"CIN_schema_{year:04d}.xsd")
