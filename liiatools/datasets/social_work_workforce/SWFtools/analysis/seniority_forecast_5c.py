import os
import pandas as pd
import numpy as np
import work_path


def seniority_forecast_5c():

    # ===== Read file ===== #
    file = 'Seniority.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    # print()
    dfSen = pd.read_csv(pathFile)


    OrgRoleDict = {
        1: "Senior Manager",
        2: "Middle Manager",
        3: "First Line Manager",
        4: "Senior Practitioner",
        5: "Case Holder",
        6: "Qualified without cases"
    }

    dfSen['OrgRoleName'] = [OrgRoleDict[item] for item in dfSen.OrgRole]

    SeniorityCodeDict = {
        1: "Newly qualified",
        2: "Early career",
        3: "Experienced",
        4: "Senior",
        5: "Agency"
    }

    dfSen['SeniorityName'] = [SeniorityCodeDict[item]
                              for item in dfSen.SeniorityCode]

    dfSen = dfSen[["YearCensus", "SWENo", "RoleStartDate", "NewOrNot", "RoleEndDate", "LeftOrNot",
                   "AgencyWorker", "OrgRole", "OrgRoleName", "SeniorityCode", "SeniorityName"]]


    # ===== Save and export file ===== #
    fileOutN = 'SeniorityComp.csv'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    dfSen.to_csv(fileOut, index=False)


    # ===== Read file ===== #
    file = 'SeniorityComp.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    dfSen = pd.read_csv(pathFile)

    dfSen = dfSen.sort_values(by=['SWENo', 'YearCensus'])

    # ============================================================================================ #


    # ===== Read file ===== #
    file = 'merged_modified.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(path, file)
    dfMerged = pd.read_csv(pathFile)

    dfMerged = dfMerged.sort_values(by=['SWENo', 'YearCensus'])

    dfMerged['OrgRole'] = dfSen['OrgRole']
    dfMerged['OrgRoleName'] = dfSen['OrgRoleName']
    dfMerged['SeniorityCode'] = dfSen['SeniorityCode']
    dfMerged['SeniorityName'] = dfSen['SeniorityName']


    # ===== Save and export file ===== #
    fileOutN = 'CompMergSen.csv'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    dfMerged.to_csv(fileOut, index=False)

    # ================================================================================================================= #


    # ===== Read file ===== #
    file = 'CompMergSen.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    df = pd.read_csv(pathFile)

    df5C = df[['LEAName', 'YearCensus', 'SeniorityCode', 'SeniorityName', 'FTE']]

    df5C = df5C.sort_values(by=['LEAName', 'YearCensus', 'SeniorityCode'])

    df5C = df.groupby(['LEAName', 'YearCensus', 'SeniorityCode', 'SeniorityName'])\
        .agg(FTESum=('FTE', 'sum'))


    # ===== Save and export file ===== #
    fileOutN = 'FTESum_5d.xlsx'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df5C.to_excel(fileOut, merge_cells=False)


# seniority_forecast_5c()
