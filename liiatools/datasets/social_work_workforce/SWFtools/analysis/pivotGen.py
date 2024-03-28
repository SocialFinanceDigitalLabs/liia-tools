"""
   Spreadsheet generation with columns ordered by: YearCensus, LEAName, Gender, Ethnicity_Compact. 
   And with the sum columns: FTESum. 
   And the employee count: SWENo_Count
"""
import os
import pandas as pd
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


def pivotGen():
    # ===== Read file ===== #
    file = "merged_modified.csv"
    path = work_path.flatfile_folder
    requestPath = work_path.request
    pathFile = os.path.join(path, file)
    df = pd.read_csv(pathFile)

    pivotTable = df.groupby(
        ["YearCensus", "LEAName", "Gender", "Ethnicity_Compact"]
    ).agg(FTESum=("FTE", "sum"), SWENo_Count=("SWENo", "count"))

    # ===== Save and export file ===== #
    fileOutN = "pivotTable.xlsx"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    pivotTable.to_excel(fileOut, merge_cells=False)
