import os
import pandas as pd
import numpy as np
import work_path


def FTESum_2020():

  # ===== Read file ===== #
    file = 'CompMergSen.csv'
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    df = pd.read_csv(pathFile)

    df2020 = df[df["YearCensus"] == 2020]

    df5D = df2020[['LEAName', 'YearCensus',
                   'SeniorityCode', 'SeniorityName', 'FTE']]

    df5D = df2020.groupby(['LEAName', 'YearCensus', 'SeniorityCode', 'SeniorityName'])\
        .agg(FTESum=('FTE', 'sum'))

    # ===== Save and export file ===== #
    fileOutN = 'FTESum_2020.xlsx'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df5D.to_excel(fileOut, merge_cells=False)
