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
    print("create_previous_and_next_episode()...")
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


def add_latest_year_and_source_for_la(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to containing latest submission year for each LA

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing latest submission year for each LA
    """
    print("add_latest_year_for_la()")
    dataframe['YEAR_latest'] = dataframe.groupby('LA')['YEAR'].transform('max')
    dataframe["Episode_source"] = "Original"
    return dataframe


def identify_not_latest_open_episode(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to identify rows with open episodes that were not submitted in the latest file year

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing true if episode is open but is not from the latest file year
    """
    print("identify_not_latest_open_episode()...TODO")
    return dataframe