from typing import Dict, List
import pandas as pd

from liiatools.common.spec.__data_schema import Column


def to_dataframe(data: List[Dict], table_config: Dict[str, Column]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    for column_name, column_spec in table_config.items():
        if column_spec.type == "date":
            # Set dtype on date columns
            df[column_name] = pd.to_datetime(df[column_name], errors="raise").dt.date
        elif column_spec.type == "category":
            # set type to categorical
            df[column_name] = df[column_name].astype("category")
    return df
