import pandas as pd


def create_demographic_table(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the sum of FTE and count of SWENo by YEAR, LA, GenderCurrent, Ethnicity, SeniorityCode, NewOrNot
    and LeftOrNot from the input dataframe

    :param data: Dataframe containing input data
    :return: Dataframe of the sum of FTE and count of SWENo grouped by YEAR, LA, GenderCurrent and Ethnicity
    """
    return data.groupby(
        [
            "Year",
            "LA",
            "GenderCurrent",
            "Ethnicity",
            "SeniorityCode",
            "NewOrNot",
            "LeftOrNot",
        ],
        as_index=False,
    ).agg(FTESum=("FTE", "sum"), SWENo_Count=("SWENo", "count"))
