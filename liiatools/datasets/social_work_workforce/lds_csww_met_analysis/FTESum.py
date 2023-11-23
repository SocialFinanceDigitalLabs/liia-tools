import pandas as pd


def FTESum(data: pd.DataFrame, year=None) -> pd.DataFrame:
    """
    Calculate the sum of FTE by LA, YEAR, SeniorityCode and SeniorityName from the input dataframe

    :param data: Dataframe containing input data
    :param year: Year to filter the data on if required
    :return: Dataframe of the sum of FTE grouped by LA, YEAR, SeniorityCode, SeniorityName
    """
    if year is not None:
        data = data[data["YEAR"] == year]

    data = data.groupby(["LA", "YEAR", "SeniorityCode", "SeniorityName"], as_index=False).agg(
        FTESum=("FTE", "sum")
    )
    return data
