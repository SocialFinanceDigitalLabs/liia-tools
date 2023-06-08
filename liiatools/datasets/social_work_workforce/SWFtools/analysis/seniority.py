import os
import pandas as pd
import numpy as np
from liiatools.datasets.social_work_workforce.SWFtools.dataprocessing.converter import (
    ORG_ROLE_DICT,
    SENIORITY_CODE_DICT,
)
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


def seniority():
    # ===== Read file ===== #
    file = "merged_modified.csv"
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(path, file)
    df = pd.read_csv(pathFile)

    df = df[
        [
            "YearCensus",
            "AgencyWorker",
            "SWENo",
            "RoleStartDate",
            "RoleEndDate",
            "OrgRole",
        ]
    ]

    listNew = []
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        if row["RoleStartDate"] == row["YearCensus"]:
            listNew.append("New")
        else:
            listNew.append("Not")

    df["NewOrNot"] = listNew

    df = df[
        [
            "YearCensus",
            "AgencyWorker",
            "SWENo",
            "RoleStartDate",
            "RoleEndDate",
            "OrgRole",
            "NewOrNot",
        ]
    ]

    listLeft = []
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        if row["RoleEndDate"] == row["YearCensus"]:
            listLeft.append("Left")
        else:
            listLeft.append("Not")

    df["LeftOrNot"] = listLeft

    df = df[
        [
            "YearCensus",
            "AgencyWorker",
            "SWENo",
            "RoleStartDate",
            "RoleEndDate",
            "OrgRole",
            "NewOrNot",
            "LeftOrNot",
        ]
    ]

    listSen = []
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        if row["RoleStartDate"] == row["YearCensus"]:
            listSen.append(1)
        elif row["AgencyWorker"] == 1:
            listSen.append(5)
        elif row["OrgRole"] == 5 or row["OrgRole"] == 6:
            listSen.append(2)
        elif row["OrgRole"] == 2 or row["OrgRole"] == 3 or row["OrgRole"] == 4:
            listSen.append(3)
        elif row["OrgRole"] == 1:
            listSen.append(4)

    df["SeniorityCode"] = listSen

    df = df[
        [
            "YearCensus",
            "SWENo",
            "RoleStartDate",
            "NewOrNot",
            "RoleEndDate",
            "LeftOrNot",
            "AgencyWorker",
            "OrgRole",
            "SeniorityCode",
        ]
    ]

    fileOutN = "Seniority.csv"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df.to_csv(fileOut, index=False)


def seniority_forecast_04():
    # ===== Read file ===== #
    file = "FTESum_2020.xlsx"
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    dfSen = pd.read_excel(pathFile)

    # ===== Rename column ===== #
    dfSen.rename(columns={"FTESum": "2020"}, inplace=True)

    # ===== Read file ===== #
    file = "population_growth_table.xlsx"
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    p_df = pd.read_excel(pathFile)

    countYearBefore = 2019
    countYearNext = 2020
    for count in range(5):
        countYearBefore = countYearBefore + 1
        countYearNext = countYearNext + 1
        # Havering
        dfSen.loc[dfSen["LEAName"] == "Havering", str(countYearNext)] = (
            dfSen[str(countYearBefore)] / p_df.loc[0, str(countYearBefore)]
        ) * p_df.loc[0, str(countYearNext)]
        # Barking and Dagenham
        dfSen.loc[dfSen["LEAName"] == "Barking and Dagenham", str(countYearNext)] = (
            dfSen[str(countYearBefore)] / p_df.loc[1, str(countYearBefore)]
        ) * p_df.loc[1, str(countYearNext)]
        # Redbridge
        dfSen.loc[dfSen["LEAName"] == "Redbridge", str(countYearNext)] = (
            dfSen[str(countYearBefore)] / p_df.loc[2, str(countYearBefore)]
        ) * p_df.loc[2, str(countYearNext)]
        # Newham
        dfSen.loc[dfSen["LEAName"] == "Newham", str(countYearNext)] = (
            dfSen[str(countYearBefore)] / p_df.loc[3, str(countYearBefore)]
        ) * p_df.loc[3, str(countYearNext)]
        # Tower Hamlets
        dfSen.loc[dfSen["LEAName"] == "Tower Hamlets", str(countYearNext)] = (
            dfSen[str(countYearBefore)] / p_df.loc[4, str(countYearBefore)]
        ) * p_df.loc[4, str(countYearNext)]
        # Waltham Forest
        dfSen.loc[dfSen["LEAName"] == "Waltham Forest", str(countYearNext)] = (
            dfSen[str(countYearBefore)] / p_df.loc[5, str(countYearBefore)]
        ) * p_df.loc[5, str(countYearNext)]

    dfSen["2020"] = dfSen["2020"].round(3)
    dfSen["2021"] = dfSen["2021"].round(3)
    dfSen["2022"] = dfSen["2022"].round(3)
    dfSen["2023"] = dfSen["2023"].round(3)
    dfSen["2024"] = dfSen["2024"].round(3)
    dfSen["2025"] = dfSen["2025"].round(3)

    dfSen = dfSen.drop(["YearCensus"], axis=1)

    # ===== Save and export file ===== #
    fileOutN = "seniority_forecast_04_clean.xlsx"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    dfSen.to_excel(fileOut, index=False, merge_cells=False)


def seniority_forecast_5c():
    # ===== Read file ===== #
    file = "Seniority.csv"
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    dfSen = pd.read_csv(pathFile)

    dfSen["OrgRoleName"] = dfSen.OrgRole.map(
        {int(key): ORG_ROLE_DICT[key] for key in ORG_ROLE_DICT}
    )
    dfSen["SeniorityName"] = dfSen.SeniorityCode.map(
        {int(key): SENIORITY_CODE_DICT[key] for key in SENIORITY_CODE_DICT}
    )

    dfSen = dfSen[
        [
            "YearCensus",
            "SWENo",
            "RoleStartDate",
            "NewOrNot",
            "RoleEndDate",
            "LeftOrNot",
            "AgencyWorker",
            "OrgRole",
            "OrgRoleName",
            "SeniorityCode",
            "SeniorityName",
        ]
    ]

    # ===== Save and export file ===== #
    fileOutN = "SeniorityComp.csv"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    dfSen.to_csv(fileOut, index=False)

    # ===== Sort values ===== #
    dfSen = dfSen.sort_values(by=["SWENo", "YearCensus"])

    # ============================================================================================ #

    # ===== Read file ===== #
    file = "merged_modified.csv"
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(path, file)
    dfMerged = pd.read_csv(pathFile)

    dfMerged = dfMerged.sort_values(by=["SWENo", "YearCensus"])

    dfMerged["OrgRole"] = dfSen["OrgRole"]
    dfMerged["OrgRoleName"] = dfSen["OrgRoleName"]
    dfMerged["SeniorityCode"] = dfSen["SeniorityCode"]
    dfMerged["SeniorityName"] = dfSen["SeniorityName"]

    # ===== Save and export file ===== #
    fileOutN = "CompMergSen.csv"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    dfMerged.to_csv(fileOut, index=False)

    # ================================================================================================================= #


def progressed():
    # ===== Read file ===== #
    file = "Seniority.csv"
    requestPath = work_path.request
    pathSen = os.path.join(requestPath, file)
    df = pd.read_csv(pathSen)
    df = df.sort_values(by=["SWENo", "YearCensus"])

    df["Progress"] = np.where(
        df["SWENo"] == df["SWENo"].shift(),
        np.where(
            df["SeniorityCode"] == df["SeniorityCode"].shift(), "No", "Progressed"
        ),
        "Unknown",
    )

    fileOutN = "CompletProgressed.csv"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df.to_csv(fileOut, index=False)
