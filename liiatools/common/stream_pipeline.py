from typing import Dict, List
import pandas as pd
import xml.etree.ElementTree as ET

from liiatools.common.spec.__data_schema import Column


def to_dataframe(data: List[Dict], table_config: Dict[str, Column]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    for column_name, column_spec in table_config.items():
        if column_spec.type == "date":
            # Set dtype on date columns
            df[column_name] = pd.to_datetime(df[column_name], errors="raise").dt.date
        elif column_spec.type == "time":
            # Set type on time columns
            df[column_name] = pd.to_datetime(df[column_name], errors="raise").dt.time
        elif column_spec.type == "category":
            # set type to categorical
            df[column_name] = df[column_name].astype("category")
        elif column_spec.type == "numeric":
            if column_spec.numeric.type == "integer":
                # set type to Int64
                df[column_name] = pd.to_numeric(df[column_name], errors="raise").astype(
                    "Int64"
                )
            if column_spec.numeric.type == "float":
                # set type to float
                df[column_name] = pd.to_numeric(df[column_name], errors="raise").astype(
                    "float"
                )
    return df


def to_dataframe_xml(data: List[Dict], table_config) -> pd.DataFrame:
    df = pd.DataFrame(data)
    xsd_xml = ET.parse(table_config)

    for column_name in df.columns:
        search_elem = (
            f".//{{http://www.w3.org/2001/XMLSchema}}element[@name='{column_name}']"
        )
        element = xsd_xml.find(search_elem)

        if element is not None:
            column_type = element.attrib.get("type", None)
            if column_type is not None:
                if column_type == "xs:date":
                    # Set dtype on date columns
                    df[column_name] = pd.to_datetime(
                        df[column_name], errors="raise"
                    ).dt.date
                elif column_type == "positiveintegertype":
                    # set type to Int64
                    df[column_name] = pd.to_numeric(
                        df[column_name], errors="raise"
                    ).astype("Int64")
                elif column_type[-4:] == "type":
                    # set type to categorical
                    df[column_name] = df[column_name].astype("category")
    return df
