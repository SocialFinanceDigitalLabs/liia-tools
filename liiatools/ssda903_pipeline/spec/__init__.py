import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent


class Category(BaseModel):
    """
    Representes a categorical value in a column. It can be looked up by name or code by using the `in` operator,

    e.g. `if "B" in category: ...` or `if "Banana" in category: ...`
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    name: str = None

    def model_post_init(self, __context: Any):
        self.__values = {self.code.lower()}
        if self.name:
            self.__values.add(self.name.lower())

        self._is_numeric = self.code.isnumeric() or (
            self.name and self.name.isnumeric()
        )

    def __contains__(self, item):
        if item in self.__values:
            return True

        # If one of the categories are numeric, then we try to see if we can convert the item to a number if we didn't get any direct hits
        if self._is_numeric:
            try:
                int_value = str(int(float(item)))
                if int_value in self.__values:
                    return True
            except (TypeError, ValueError):
                pass

        return False


class Column(BaseModel):
    model_config = ConfigDict(extra="forbid")

    string: Literal["alphanumeric", "postcode"] = None
    numeric: Literal["integer"] = None
    date: str = None

    dictionary: Dict = None
    category: List[Category] = None

    canbeblank: bool = True

    def model_post_init(self, __context: Any):
        if self.string == "alphanumeric":
            self.__type = "string"
        elif self.string == "postcode":
            self.__type = "postcode"
        elif self.numeric == "integer":
            self.__type = "integer"
        elif self.date:
            self.__type = "date"
        elif self.category:
            self.__type = "category"
        else:
            raise ValueError("Unknown data type")

    @property
    def type(self):
        return self.__type

    def match_category(self, value: str) -> Optional[Category]:
        assert self.category, "Column is not a category"

        value = value.strip().lower()
        for category in self.category:
            if value in category:
                return category.code
        return None


class DataSchema(BaseModel):
    column_map: Dict[str, Dict[str, Column]]

    def get_table_from_headers(self, headers: Iterable[str]) -> Optional[str]:
        """
        Given a set of column names, finds the first table where all the table columns are contained
        with the headers.

        If no table is found, returns None
        """
        headers = set(headers)
        for table_name, table_config in self.column_map.items():
            table_columns = set(table_config.keys())
            if table_columns.issubset(headers):
                return table_name
        return None

    @property
    def table(self) -> Dict[str, Dict[str, Column]]:
        """
        Alias for column_map to make code a bit more readable
        """
        return self.column_map


@lru_cache
def load_schema(year: int) -> DataSchema:
    pattern = re.compile(r"SSDA903_schema_(\d{4})(\.diff)?\.yml")

    # Build index of all schema files
    all_schema_files = list(SCHEMA_DIR.glob("SSDA903_schema_*.yml"))
    schema_lookup = []
    for fn in all_schema_files:
        match = pattern.match(fn.name)
        assert match, f"Unexpected schema name {fn}"
        schema_lookup.append((fn, int(match.group(1)), match.group(2) is not None))

    # Filter only those earlier than the year we're looking for
    schema_lookup = [x for x in schema_lookup if x[1] <= year]

    # If we have no schema files, raise an error
    if not schema_lookup:
        raise ValueError(f"No schema files found for year {year}")

    # Find the latest complete schema
    last_complete_schema = [x for x in schema_lookup if not x[2]][-1]

    # Now filter down to only include last complete and any diff files after that
    schema_lookup = [x for x in schema_lookup if x[1] >= last_complete_schema[1]]

    # We load the full schema
    logger.debug("Loading schema from %s", schema_lookup[0][0])
    full_schema = yaml.safe_load(schema_lookup[0][0].read_text())

    # Now loop over diff files and apply them
    for fn, _, _ in schema_lookup[1:]:
        logger.debug("Loading partial schema from %s", fn)
        try:
            diff = yaml.safe_load(fn.read_text())
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing diff file {fn}") from e

        for key, diff_obj in diff.items():
            diff_type = diff_obj["type"]
            assert diff_type in ("add", "modify"), f"Unknown diff type {diff_type}"
            path = key.split(".")
            parent = full_schema
            for item in path[:-1]:
                parent = parent[item]

            parent[path[-1]] = diff_obj["value"]

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema.model_validate(full_schema)
