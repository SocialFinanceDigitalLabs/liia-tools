import re
import pandas as pd
import numpy as np
from liiatools.csww_pipeline.met_analysis.converter import (
    ORG_ROLE_DICT,
    SENIORITY_CODE_DICT,
)


def add_new_or_not_column(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add a column indicating whether a worker is new or not

    :param data: Dataframe containing input data
    :return: Dataframe with column NewOrNot added
    """
    list_new_or_not = []
    for index, row in data.iterrows():
        if row["RoleStartDate"].year == row["Year"]:
            list_new_or_not.append("New")
        else:
            list_new_or_not.append("Not")

    data["NewOrNot"] = list_new_or_not

    return data


def add_left_or_not_column(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add a column indicating whether a worker has left or not

    :param data: Dataframe containing input data
    :return: Dataframe with column LeftOrNot added
    """
    list_left_or_not = []
    for index, row in data.iterrows():
        if row["RoleEndDate"].year == row["Year"]:
            list_left_or_not.append("Left")
        else:
            list_left_or_not.append("Not")

    data["LeftOrNot"] = list_left_or_not

    return data


def add_seniority_column(data: pd.DataFrame) -> pd.DataFrame:
    """
    Assign a seniority code to each worker based on the role start date, agency worker status and org role.

    :param data: Dataframe containing input data
    :return: Dataframe with column SeniorityCode added
    """
    list_seniority = []
    for index, row in data.iterrows():
        if row["RoleStartDate"].year == row["Year"]:
            list_seniority.append(1)
        elif row["AgencyWorker"] == "1":
            list_seniority.append(5)
        elif row["OrgRole"] == "5" or row["OrgRole"] == "6":
            list_seniority.append(2)
        elif row["OrgRole"] == "2" or row["OrgRole"] == "3" or row["OrgRole"] == "4":
            list_seniority.append(3)
        elif row["OrgRole"] == "1":
            list_seniority.append(4)

    data["SeniorityCode"] = list_seniority

    return data


def add_seniority_and_retention_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compile the adding column functions
    """
    data = add_new_or_not_column(data)
    data = add_left_or_not_column(data)
    data = add_seniority_column(data)
    return data


def _find_min_max_year_population_growth_table(
    population_growth_table: pd.DataFrame,
) -> tuple[int, int]:
    """
    Calculate the minimum and maximum years from column headers in a dataframe

    :param population_growth_table: Dataframe containing population levels across multiple years
    :return: Minimum and maximum year from the column headers
    """
    years = []
    for value in population_growth_table.columns:
        if re.findall(r"20\d{2}", value):
            years.append(int(value))

    min_year = min(years)
    max_year = max(years)
    return min_year, max_year


def seniority_forecast(
    data: pd.DataFrame, population_growth_table: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate the seniority forecast for LAs in the population_growth_table for the number of years given in the
    population_growth_table

    The seniority forecast is calculated by multiplying the FTESum from the data by the population growth rate
    for each year and LA from the population_growth_table

    :param data: Dataframe containing input data
    :param population_growth_table: Dataframe containing population levels of LAs across several years
    :return: Dataframe with the forecasted FTESum per LA across the number of years in the population_growth_table
    """
    min_year, max_year = _find_min_max_year_population_growth_table(
        population_growth_table
    )
    boroughs = population_growth_table["LEAName"].tolist()

    data = data.rename(columns={"FTESum": str(min_year)})

    for borough in boroughs:
        for year in range(min_year, max_year):
            data.loc[data["LA"] == borough, str(year + 1)] = (
                data[str(year)]
                / float(
                    population_growth_table.loc[
                        population_growth_table["LEAName"] == borough, str(year)
                    ]
                )
                * float(
                    population_growth_table.loc[
                        population_growth_table["LEAName"] == borough, str(year + 1)
                    ]
                )
            )

    data = data.round(3)
    data = data.drop(["Year"], axis=1)
    return data


def convert_codes_to_names(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns to the dataframe converting OrgRole and SeniorityCode into their respective string values

    :param data: Dataframe containing input data
    :return: Dataframe with OrgRoleName and SeniorityName added
    """
    data["OrgRoleName"] = data.OrgRole.map(
        {int(key): ORG_ROLE_DICT[key] for key in ORG_ROLE_DICT}
    )
    data["SeniorityName"] = data.SeniorityCode.map(
        {int(key): SENIORITY_CODE_DICT[key] for key in SENIORITY_CODE_DICT}
    )

    return data


def progressed(data: pd.DataFrame) -> pd.DataFrame:
    """
    Determine whether an employee has progressed in their seniority code from the previous year

    :param data: Dataframe containing input data
    :return: Dataframe with Progress column added
    """
    data = data.sort_values(by=["SWENo", "Year"])

    data["Progress"] = np.where(
        data["SWENo"] == data["SWENo"].shift(),
        np.where(
            data["SeniorityCode"] == data["SeniorityCode"].shift(), "No", "Progressed"
        ),
        "Unknown",
    )
    return data
