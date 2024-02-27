import numpy as np
import pandas as pd
from datetime import datetime, timedelta

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

__COLUMNS_TO_KEEP = [
    "CHILD",
    "LA",
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
    "YEAR_latest",
    "Episode_source",
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
    
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime, format="%Y-%m-%d", errors="raise")
    return dataframe


def add_latest_year_and_source_for_la(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to containing latest submission year and source for each LA

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing latest submission year for each LA and column showing episode source
    """
    source_for_episode_row = "Original"
    print("add_latest_year_and_source_for_la()...")
    dataframe['YEAR_latest'] = dataframe.groupby('LA')['YEAR'].transform('max')
    dataframe["Episode_source"] = source_for_episode_row
    return dataframe


def _is_next_episode_duplicate(row):
    return (row.DEC.isnull() &
            row.Has_next_episode &
            ( (row.DECOM_next != row.DECOM) | (row.DECOM_next.isnull() & row.DECOM.isnull()) ) &
            ( (row.RNE_next == row.RNE) | (row.RNE_next.isnull() &  row.RNE.isnull()) ) &
            ( (row.LS_next == row.LS) | (row.LS_next.isnull() & row.LS.isnull()) ) &
            ( (row.PLACE_next == row.PLACE) | (row.PLACE_next.isnull() | row.PLACE.isnull()) ) &
            ( (row.PLACE_PROVIDER_next == row.PLACE_PROVIDER) | (row.PLACE_PROVIDER_next.isnull() | row.PLACE_PROVIDER.isnull()) ) &
            ( (row.PL_POST_next == row.PL_POST) | (row.PL_POST_next.isnull() | row.PL_POST.isnull()) ) &
            ( (row.URN_next == row.URN) | (row.URN_next.isnull() | row.URN.isnull()) )
            )


def _is_previous_episode_duplicate(row):
    return (row.DEC.isnull() &
            row.Has_previous_episode &
            ( (row.DECOM_previous != row.DECOM) | (row.DECOM_previous.isnull() & row.DECOM.isnull()) ) &
            ( (row.RNE_previous == row.RNE) | (row.RNE_previous.isnull() &  row.RNE.isnull()) ) &
            ( (row.LS_previous == row.LS) | (row.LS_previous.isnull() & row.LS.isnull()) ) &
            ( (row.PLACE_previous == row.PLACE) | (row.PLACE_previous.isnull() | row.PLACE.isnull()) ) &
            ( (row.PLACE_PROVIDER_previous == row.PLACE_PROVIDER) | (row.PLACE_PROVIDER_previous.isnull() | row.PLACE_PROVIDER.isnull()) ) &
            ( (row.PL_POST_previous == row.PL_POST) | (row.PL_POST_previous.isnull() | row.PL_POST.isnull()) ) &
            ( (row.URN_previous == row.URN) | (row.URN_previous.isnull() | row.URN.isnull()) )
            )


def _is_previous_episode_submitted_later(row):
    return (row.DEC.isnull() &
            (row.Has_previous_episode) &
            (row.YEAR_previous > row.YEAR)
            )

def _stage1_rule_to_apply(row):
    if row["Has_open_episode_error"]:
        if row["Next_episode_is_duplicate"] | row["Previous_episode_is_duplicate"]:
            return "RULE_3" # Duplicate
        if row["Previous_episode_submitted_later"]:
            return "RULE_3A" # Episode replaced in later submission
        if row["Has_next_episode"] is False:
            return "RULE_2" # Ceases LAC
        if row["Has_next_episode_with_RNE_equals_S"]:
            return "RULE_1A" # Ceases LAC, but re-enters care later
        return "RULE_1" # Remains LAC, episode changes


def add_stage1_rule_identifier_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns to identify rows with open episodes that meet certain criteria

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with columns showing true if certain conditions are met
    """
    print("add_stage1_rule_identifier_columns...")
    dataframe = dataframe.assign(Has_open_episode_error=lambda row: (row.DEC.isnull() ) & (row.YEAR != row.YEAR_latest) )
    dataframe["Has_next_episode"] = dataframe["DECOM_next"].notnull()
    dataframe["Has_previous_episode"] = dataframe["DECOM_previous"].notnull()
    dataframe = dataframe.assign(Has_next_episode_with_RNE_equals_S=lambda row: (row.Has_next_episode) & (row.RNE_next == "S") )
    dataframe = dataframe.assign(Next_episode_is_duplicate=_is_next_episode_duplicate)
    dataframe = dataframe.assign(Previous_episode_is_duplicate=_is_previous_episode_duplicate)
    dataframe = dataframe.assign(Previous_episode_submitted_later=_is_previous_episode_submitted_later)
    return dataframe


def identify_stage1_rule_to_apply(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to identify which stage 1 rule should be applied:
    RULE_1 : Child remains LAC but episode changes
    RULE_1A: Child ceases LAC but re-enters care later
    RULE_2 : Child ceases LAC
    RULE_3 : Episode is a duplicate - delete
    RULE_3A: Episode replaced in later submission - delete

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing stage 1 rule to be applied
    """
    print("identify_stage1_rule_to_apply...")
    dataframe["Rule_to_apply"] = dataframe.apply(_stage1_rule_to_apply, axis=1)
    return dataframe


def _update_dec_stage1(row):
    """
    Determine updated DEC value. Defaults to input DEC if no rule to apply
    :param row: Row from dataframe with SSDA903 Episodes data
    :return: Updated DEC date
    """
    end_of_year = datetime(row["YEAR"], 3, 31)
    if row["Has_open_episode_error"]:
        if row["Rule_to_apply"] == "RULE_1":
            return row["DECOM_next"]
        if row["Rule_to_apply"] == "RULE_1A":
            day_before_next_decom = row["DECOM_next"] - timedelta(days = 1)
            return min(end_of_year, day_before_next_decom) # get earliest date
        if row["Rule_to_apply"] == "RULE_2":
            return end_of_year
    return row["DEC"]


def _update_rec_stage1(row):
    """
    Determine updated REC value. Defaults to input REC if no rule to apply
    :param row: Row from dataframe with SSDA903 Episodes data
    :return: Updated REC value
    """
    episode_ends_liia_fix = "E99"
    episode_continues = "X1"
    if row["Has_open_episode_error"]:
        if row["Rule_to_apply"] == "RULE_1":
            return episode_continues
        if row["Rule_to_apply"] in ("RULE_1A", "RULE_2"):
            return episode_ends_liia_fix
    return row["REC"]


def _update_reason_place_change_stage1(row):
    """
    Determine updated REASON_PLACE_CHANGE value. Defaults to input value if no rule to apply
    :param row: Row from dataframe with SSDA903 Episodes data
    :return: Updated REASON_PLACE_CHANGE value
    """
    reason_liia_fix = "LIIAF"
    if row["Has_open_episode_error"]:
        if (row["Rule_to_apply"] == "RULE_1") & (row["RNE_next"] in ("P", "B", "T", "U")):
            return reason_liia_fix
    return row["REASON_PLACE_CHANGE"]


def _update_episode_source_stage1(row):
    """
    Determine updated Episode_source value. Defaults to input value if no rule to apply
    :param row: Row from dataframe with SSDA903 Episodes data
    :return: Updated Episode_source value
    """
    if row["Has_open_episode_error"]:
        return row["Rule_to_apply"]
    return row["Episode_source"]


def apply_stage1_rules(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Apply stage 1 rules:
    RULE_1 : Child remains LAC but episode changes
    RULE_1A: Child ceases LAC but re-enters care later
    RULE_2 : Child ceases LAC
    RULE_3 : Episode is a duplicate - delete
    RULE_3A: Episode replaced in later submission - delete

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with stage 1 rules applied
    """
    print("apply_stage1_rules")
    # Apply rules 3, 3A to delete rows
    episodes_to_delete = dataframe["Rule_to_apply"].isin( ['RULE_3', 'RULE_3A'])
    dataframe = dataframe.drop(dataframe[episodes_to_delete].index)

    # Apply rules 1, 1A, 2
    dataframe["DEC"] = dataframe.apply(_update_dec_stage1, axis=1)
    dataframe["REC"] = dataframe.apply(_update_rec_stage1, axis=1)
    dataframe["REASON_PLACE_CHANGE"] = dataframe.apply(_update_reason_place_change_stage1, axis=1)
    dataframe["Episode_source"] = dataframe.apply(_update_episode_source_stage1, axis=1)

    return dataframe


def _overlaps_next_episode(row):
    if row["Has_next_episode"]:
        return (row.YEAR < row.YEAR_next) & (row.DEC > row.DECOM_next)
    return False


def _has_x1_gap_before_next_episode(row):
    if row["Has_next_episode"]:
        return (row.YEAR < row.YEAR_next) & (row.DEC < row.DECOM_next) & (row.REC == "X1")
    return False


def _stage2_rule_to_apply(row):
    if row["Overlaps_next_episode"]:
        return "RULE_4" # Overlaps next episode and next episode was submitted later
    if row["Has_X1_gap_before_next_episode"]:
        return "RULE_5" # Ends before next episode but has reason "X1" - continuous and next ep was submitted later


def _update_dec_stage2(row):
    """
    Determine updated DEC value. Defaults to input DEC if no rule to apply
    :param row: Row from dataframe with SSDA903 Episodes data
    :return: Updated DEC date
    """
    if (row["Rule_to_apply"]=="RULE_4") | (row["Rule_to_apply"]=="RULE_5"):
        return row["DECOM_next"]
    return row["DEC"]


def _update_episode_source_stage2(row):
    """
    Determine updated Episode_source value. Defaults to input value if no rule to apply
    :param row: Row from dataframe with SSDA903 Episodes data
    :return: Updated Episode_source value
    """
    if (row["Rule_to_apply"]=="RULE_4") | (row["Rule_to_apply"]=="RULE_5"):
        if row["Episode_source"] == "Original":
            return row["Rule_to_apply"]
        return row["Episode_source"] & " | " & row["Rule_to_apply"]
    return row["Episode_source"]


def add_stage2_rule_identifier_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns to identify rows which overlap or underlap surrounding episodes

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with columns showing true if certain conditions are met
    """
    print("add_stage2_rule_identifier_columns")
    dataframe["Has_next_episode"] = dataframe["DECOM_next"].notnull()
    dataframe["Overlaps_next_episode"] = dataframe.apply(_overlaps_next_episode, axis=1)
    dataframe["Has_X1_gap_before_next_episode"] = dataframe.apply(_has_x1_gap_before_next_episode, axis=1)
    return dataframe


def identify_stage2_rule_to_apply(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Add column to identify which stage 2 rule should be applied:
    RULE_4: Overlaps with next episode
    RULE_5: End reason is "X1" - episode continues - but there is gap before next episode

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with column showing stage 2 rule to be applied
    """
    print("identify_stage2_rule_to_apply")
    dataframe["Rule_to_apply"] = dataframe.apply(_stage2_rule_to_apply, axis=1)
    dataframe["Episode_source"] = dataframe.apply(_update_episode_source_stage2, axis=1)
    return dataframe


def apply_stage2_rules(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Apply stage 2 rules:
    RULE_4: Overlaps with next episode
    RULE_5: End reason is "X1" - episode continues - but there is gap before next episode

    :param dataframe: Dataframe with SSDA903 Episodes data
    :return: Dataframe with stage 2 rules applied
    """
    print("apply_stage2_rules")
    # Apply rules 4, 5
    dataframe["DEC"] = dataframe.apply(_update_dec_stage2, axis=1)
    return dataframe