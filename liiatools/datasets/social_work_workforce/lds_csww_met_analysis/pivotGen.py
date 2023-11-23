import pandas as pd


def pivotGen(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the sum of FTE and count of SWENo by YEAR, LA, GenderCurrent and Ethnicity from the input dataframe

    :param data: Dataframe containing input data
    :return: Dataframe of the sum of FTE and count of SWENo grouped by YEAR, LA, GenderCurrent and Ethnicity
    """
    return data.groupby(
        ["YEAR", "LA", "GenderCurrent", "Ethnicity"], as_index=False
    ).agg(FTESum=("FTE", "sum"), SWENo_Count=("SWENo", "count"))

