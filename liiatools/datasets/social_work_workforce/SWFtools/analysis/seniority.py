import pandas as pd
import numpy as np
import os
from pathlib import Path
import work_path


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


# seniority()
