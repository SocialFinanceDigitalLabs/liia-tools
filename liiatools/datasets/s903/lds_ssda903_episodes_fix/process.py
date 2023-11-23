import numpy as np
import pandas as pd

__COLUMNS = [
    "DECOM",
    "RNE",
    "LS",
    "CIN",
    "PLACE",
    "PLACE_PROVIDER",
    "DEC",
    "REC",
    "REASON_PLACE_CHANGE",
    "HOME_POST",
    "PL_POST",
    "URN",
    "YEAR",
]


def create_previous_and_next_episode(dataframe: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Add previous and next episode information to each line of a dataframe

    :param dataframe: Dataframe with SSDA903 Episodes data
    :param columns: List of columns containing required data from previous/next episodes
    :return: Dataframe with columns showing previous and next episodes
    """
    for column in columns:
        dataframe[column + "_previous"] = np.where(
            dataframe["CHILD"] == dataframe["CHILD"].shift(1),
            dataframe[column].shift(1),
            None,
        )
        dataframe[column + "_next"] = np.where(
            dataframe["CHILD"] == dataframe["CHILD"].shift(-1),
            dataframe[column].shift(-1),
            None,
        )
    return dataframe
