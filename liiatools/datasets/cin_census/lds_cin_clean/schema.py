from functools import cached_property
from pathlib import Path

import xmlschema

from liiatools.spec import cin_census as cin_asset_dir


class Schema:
    def __init__(self, year):
        self.__year = year

    @cached_property
    def schema(self) -> xmlschema.XMLSchema:
        return xmlschema.XMLSchema(
            Path(cin_asset_dir.__file__).parent / f"CIN_schema_{self.__year}.xsd"
        )
