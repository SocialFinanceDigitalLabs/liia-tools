from functools import cached_property
from pathlib import Path

import xmlschema

from liiatools.spec import social_work_workforce as csww_asset_dir


class Schema:
    def __init__(self, year):
        self.__year = year

    @cached_property
    def schema(self) -> xmlschema.XMLSchema:
        return xmlschema.XMLSchema(
            Path(csww_asset_dir.__file__).parent / f"csww_schema_{self.__year}.xsd"
        )
