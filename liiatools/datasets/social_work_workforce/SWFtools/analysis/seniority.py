import os
import pandas as pd
import numpy as np
from liiatools.datasets.social_work_workforce.SWFtools.dataprocessing.converter import (ORG_ROLE_DICT,
                                                                                        SENIORITY_CODE_DICT)
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


def seniority():
    # ===== Read file ===== #
    file = 'merged_modified.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(path, file)
    # print()
    df = pd.read_csv(pathFile)

    df = df[['YearCensus', 'AgencyWorker', 'SWENo',
             'RoleStartDate', 'RoleEndDate', 'OrgRole']]

    listNew = []
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        if row['RoleStartDate'] == row['YearCensus']:
            # print("New")
            listNew.append('New')
        else:
            # print("Not")
            listNew.append('Not')

    # print(listNew)
    df['NewOrNot'] = listNew

    df = df[['YearCensus', 'AgencyWorker', 'SWENo',
             'RoleStartDate', 'RoleEndDate', 'OrgRole', 'NewOrNot']]

    listLeft = []
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        if row['RoleEndDate'] == row['YearCensus']:
            # print("New")
            listLeft.append('Left')
        else:
            # print("Not")
            listLeft.append('Not')

    # print(listNew)
    df['LeftOrNot'] = listLeft

    df = df[['YearCensus', 'AgencyWorker', 'SWENo', 'RoleStartDate',
             'RoleEndDate', 'OrgRole', 'NewOrNot', 'LeftOrNot']]

    listSen = []
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        if row['RoleStartDate'] == row['YearCensus']:
            # print("New")
            listSen.append(1)
        elif row['AgencyWorker'] == 1:
            # print("Not")
            listSen.append(5)
        elif row['OrgRole'] == 5 or row['OrgRole'] == 6:
            # print("Not")
            listSen.append(2)
        elif row['OrgRole'] == 2 or row['OrgRole'] == 3 or row['OrgRole'] == 4:
            # print("Not")
            listSen.append(3)
        elif row['OrgRole'] == 1:
            # print("Not")
            listSen.append(4)

    # print(listSen)
    df['SeniorityCode'] = listSen

    df = df[['YearCensus', 'SWENo', 'RoleStartDate', 'NewOrNot',
             'RoleEndDate', 'LeftOrNot', 'AgencyWorker', 'OrgRole', 'SeniorityCode']]

    # print(df)
    fileOutN = 'Seniority.csv'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df.to_csv(fileOut, index=False)


def seniority_forecast_04():

   # ===== Read file ===== #
    file = 'FTESum_2020.xlsx'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    # print()
    df01 = pd.read_excel(pathFile)

    df01['2020'] = df01['FTESum']
    df01 = df01.drop(['FTESum'], axis=1)

    #p_df = pd.read_excel('population_growth.xlsx')

    # ===== Read file ===== #
    file = 'population_growth_table.xlsx'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    p_df = pd.read_excel(pathFile)

    df_result = df01

    countYearBefore = 2019
    countYearNext = 2020
    for count in range(5):
        countYearBefore = countYearBefore + 1
        countYearNext = countYearNext + 1
        # Havering
        df_result.loc[df_result['LEAName'] == 'Havering', str(countYearNext)] = ((df_result[str(
            countYearBefore)] / p_df.loc[0, str(countYearBefore)])*p_df.loc[0, str(countYearNext)])
        # Barking and Dagenham
        df_result.loc[df_result['LEAName'] == 'Barking and Dagenham', str(countYearNext)] = (
            (df_result[str(countYearBefore)] / p_df.loc[1, str(countYearBefore)])*p_df.loc[1, str(countYearNext)])
        # Redbridge
        df_result.loc[df_result['LEAName'] == 'Redbridge', str(countYearNext)] = ((df_result[str(
            countYearBefore)] / p_df.loc[2, str(countYearBefore)])*p_df.loc[2, str(countYearNext)])
        # Newham
        df_result.loc[df_result['LEAName'] == 'Newham', str(countYearNext)] = ((df_result[str(
            countYearBefore)] / p_df.loc[3, str(countYearBefore)])*p_df.loc[3, str(countYearNext)])
        # Tower Hamlets
        df_result.loc[df_result['LEAName'] == 'Tower Hamlets', str(countYearNext)] = ((df_result[str(
            countYearBefore)] / p_df.loc[4, str(countYearBefore)])*p_df.loc[4, str(countYearNext)])
        # Waltham Forest
        df_result.loc[df_result['LEAName'] == 'Waltham Forest', str(countYearNext)] = (
            (df_result[str(countYearBefore)] / p_df.loc[5, str(countYearBefore)])*p_df.loc[5, str(countYearNext)])
      # print('teste')

    df_result['2020'] = df_result['2020'].round(3)
    df_result['2021'] = df_result['2021'].round(3)
    df_result['2022'] = df_result['2022'].round(3)
    df_result['2023'] = df_result['2023'].round(3)
    df_result['2024'] = df_result['2024'].round(3)
    df_result['2025'] = df_result['2025'].round(3)

    df_result = df_result.drop(['YearCensus'], axis=1)


    # ===== Save and export file ===== #
    fileOutN = 'seniority_forecast_04_clean.xlsx'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df_result.to_excel(fileOut, index=False, merge_cells=False)
    

def seniority_forecast_5c():

    # ===== Read file ===== #
    file = 'Seniority.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    dfSen = pd.read_csv(pathFile)


    dfSen['OrgRoleName'] = dfSen.OrgRole.map(ORG_ROLE_DICT)

    dfSen['SeniorityName'] = dfSen.SeniorityCode.map(SENIORITY_CODE_DICT)

    dfSen = dfSen[["YearCensus", "SWENo", "RoleStartDate", "NewOrNot", "RoleEndDate", "LeftOrNot",
                   "AgencyWorker", "OrgRole", "OrgRoleName", "SeniorityCode", "SeniorityName"]]


    # ===== Save and export file ===== #
    fileOutN = 'SeniorityComp.csv'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    dfSen.to_csv(fileOut, index=False)


    # ===== Sort values ===== #
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


def progressed():
  # ===== Read file ===== #
    file = 'Seniority.csv'
    requestPath = work_path.request
    pathSen = os.path.join(requestPath, file)
    # print()
    df = pd.read_csv(pathSen)
    df = df.sort_values(by=['SWENo', 'YearCensus'])

    df['Progress'] = np.where(df['SWENo'] == df['SWENo'].shift(), np.where(
        df['SeniorityCode'] == df['SeniorityCode'].shift(), 'No', 'Progressed'), 'Unknown')

    fileOutN = 'CompletProgressed.csv'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df.to_csv(fileOut, index=False)