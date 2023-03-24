from functools import cached_property
from pathlib import Path

from xmlschema import XMLSchema

from liiatools.spec import social_work_workforce as social_work_workforce_dir


class Schema:
    def __init__(self, year: int = 2022):
        self.__year = year

    @cached_property
    def schema(self) -> XMLSchema:
        return XMLSchema(
            Path(social_work_workforce_dir.__file__).parent
            / f"social_work_workforce_{self.__year}.xsd"
        )
