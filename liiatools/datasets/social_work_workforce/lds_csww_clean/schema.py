from functools import cached_property
from pathlib import Path

import xmlschema

from liiatools.spec import social_work_workforce as social_work_workforce_asset_dir


class Schema:
    def __init__(self, year):
        self.__year = year

    @cached_property
    def Schema(self) -> xmlschema.XMLSchema:
        return xmlschema.XMLSchema(
            Path(social_work_workforce_asset_dir.__file__).parent / f"social_work_workforce_schema_{self.__year}.xsd"
        )
