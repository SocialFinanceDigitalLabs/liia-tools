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

__DATES = [
    "DECOM",
    "DEC",
    "DECOM_previous",
    "DEC_previous",
    "DECOM_next",
    "DEC_next",
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


def format_datetime(dataframe: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    """
    Format date columns to datetime type

    :param dataframe: Dataframe with SSDA903 Episodes data
    :param columns: List of columns containing dates
    :return: Dataframe with date columns showing as datetime data type
    """
    print("format_datetime()...")
    
    # dataframe["DECOM"].apply(pd.to_datetime, format='%Y-%m-%d', errors='raise')
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime, format="%Y-%m-%d", errors="raise")
    return dataframe


def add_latest_year_and_source_for_la(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to containing latest submission year and source for each LA

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing latest submission year for each LA and column showing episode source
    """
    print("add_latest_year_and_source_for_la()...")
    dataframe['YEAR_latest'] = dataframe.groupby('LA')['YEAR'].transform('max')
    dataframe["Episode_source"] = "Original"
    return dataframe


def add_stage1_rule_identifier_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns to identify rows with open episodes that meet certain criteria

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with columns showing true if certain conditions are met
    """
    print("add_stage1_rule_identifier_columns...")
    dataframe = dataframe.assign(Has_open_episode_error=lambda x: (x.DEC.isnull() ) & (x.YEAR != x.YEAR_latest) )
    dataframe["Has_next_episode"] = dataframe["DECOM_next"].notnull()
    dataframe["Has_previous_episode"] = dataframe["DECOM_previous"].notnull()
    dataframe = dataframe.assign(Has_next_episode_with_RNE_equals_S=lambda x: (x.Has_next_episode) & (x.RNE_next == "S") )
    dataframe = dataframe.assign(Next_episode_is_duplicate=lambda x: (x.DEC).isnull() &
                                                                     (x.Has_next_episode) &
                                                                     (x.DECOM_next != x.DECOM) &
                                                                     (x.RNE_next == x.RNE) &
                                                                     (x.LS_next == x.LS) &
                                                                     (x.PLACE_next == x.PLACE) &
                                                                     (x.PLACE_PROVIDER_next == x.PLACE_PROVIDER) &
                                                                     (x.PL_POST_next == x.PL_POST) &
                                                                     (x.URN_next == x.URN)
                                                                    )
    dataframe = dataframe.assign(Previous_episode_is_duplicate=lambda x: (x.DEC).isnull() &
                                                                     (x.Has_previous_episode) &
                                                                     (x.DECOM_previous != x.DECOM) &
                                                                     (x.RNE_previous == x.RNE) &
                                                                     (x.LS_previous == x.LS) &
                                                                     (x.PLACE_previous == x.PLACE) &
                                                                     (x.PLACE_PROVIDER_previous == x.PLACE_PROVIDER) &
                                                                     (x.PL_POST_previous == x.PL_POST) &
                                                                     (x.URN_previous == x.URN)
                                                                    )
    dataframe = dataframe.assign(Previous_episode_submitted_later=lambda x: (x.DEC).isnull() &
                                                                     (x.Has_previous_episode) &
                                                                     (x.YEAR_previous > x.YEAR)
                                                                    )
    return dataframe


def identify_stage1_rule_to_apply(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to identify which stage 1 rule should be applied

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing stage 1 rule to be applied
    """
    print("identify_stage1_rule_to_apply...")
    # To do based on criteria identified
    return dataframe