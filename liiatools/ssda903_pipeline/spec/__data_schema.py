from typing import Any, Dict, Iterable, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class Category(BaseModel):
    """
    Representes a categorical value in a column. It can be looked up by name or code by using the `in` operator,

    e.g. `if "B" in category: ...` or `if "Banana" in category: ...`
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    name: str = None

    __values: str = Field("")

    def __init__(self, **data):
        super().__init__(**data)

    def __contains__(self, item):
        values = {self.code.lower()}
        if self.name:
            values.add(self.name.lower())

        is_numeric = self.code.isnumeric() or (self.name and self.name.isnumeric())

        if item in values:
            return True

        # If one of the categories are numeric, then we try to see if we can convert the item to a number if we didn't get any direct hits
        if is_numeric:
            try:
                int_value = str(int(float(item)))
                if int_value in values:
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

    @property
    def type(self):
        if self.string == "alphanumeric":
            return "string"
        elif self.string == "postcode":
            return "postcode"
        elif self.numeric == "integer":
            return "integer"
        elif self.date:
            return "date"
        elif self.category:
            return "category"
        else:
            raise ValueError("Unknown data type")

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
