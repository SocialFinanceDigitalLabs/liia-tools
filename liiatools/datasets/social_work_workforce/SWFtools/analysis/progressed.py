import os
import pandas as pd
import numpy as np
from pathlib import Path
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


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
    #df.to_csv('CompletProgressed.csv', index=False)


# progressed()
