from functools import cached_property
from pathlib import Path

import xmlschema

from liiatools.spec import sen2 as sen2_asset_dir


class Schema:
    def __init__(self, year):
        self.__year = year

    @cached_property
    def schema(self) -> xmlschema.XMLSchema:
        return xmlschema.XMLSchema(
            Path(sen2_asset_dir.__file__).parent / f"SEN2_schema_{self.__year}.xsd"
        )
