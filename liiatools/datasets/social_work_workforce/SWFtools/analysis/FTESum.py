import os
import pandas as pd
import numpy as np
import work_path


def FTESum():

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
