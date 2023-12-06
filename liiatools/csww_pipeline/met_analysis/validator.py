import pandas as pd


NON_AGENCY_MANDATORY_TAG = [
    "PersonBirthDate",
    "GenderCurrent",
    "Ethnicity",
    "QualInst",
    "StepUpGrad",
    "OrgRole",
    "RoleStartDate",
    "StartOrigin",
    "FTE30",
    "WorkingDaysLost",
    "FrontlineGrad",
]


def _remove_invalid_xor(
    data: pd.DataFrame, column_1: str, column_2: str
) -> pd.DataFrame:
    """
    Remove rows from a data frame if column_1 is blank and column_2 is not blank or vice versa

    :param data: A dataframe with potentially invalid rows
    :param column_1: Column name that needs to contain data if column_2 does and needs to be blank if not
    :param column_2: Column name that needs to contain data if column_1 does and needs to be blank if not
    :return: A dataframe with rows containing invalid data removed
    """
    return data.drop(data[(data[column_1].isna()) ^ (data[column_2].isna())].index)


def remove_invalid_worker_data(
    csww_df: pd.DataFrame, non_agency_mandatory_field: list
) -> pd.DataFrame:
    """
    Takes in a worker dataframe validates it and returns the worker data as a dataframe with invalid workers removed

    :param csww_df: A dataframe with children's social care workforce data
    :param non_agency_mandatory_field: A list of column names that need to contain data for non agency workers
    :return: A dataframe that contains only valid worker data
    """

    # Validation rules if the worker is a non-agency worker
    for field in non_agency_mandatory_field:
        csww_df = csww_df.drop(
            csww_df[(csww_df["AgencyWorker"] != "1") & (csww_df[field].isna())].index
        )

    # Validation rules if the worker is a starter
    csww_df = _remove_invalid_xor(csww_df, "RoleStartDate", "StartOrigin")

    # Validation rules if the worker is a leaver
    csww_df = csww_df.drop(
        csww_df[(csww_df["RoleEndDate"].notna()) & ~(csww_df["FTE"] > 0)].index
    )
    csww_df = _remove_invalid_xor(csww_df, "RoleEndDate", "ReasonLeave")
    csww_df = _remove_invalid_xor(csww_df, "RoleEndDate", "LeaverDestination")

    return csww_df
