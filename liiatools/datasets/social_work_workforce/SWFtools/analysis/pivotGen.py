"""
   Spreadsheet generation with columns ordered by: YearCensus, LEAName, Gender, Ethnicity_Compact. 
   And with the sum columns: FTESum. 
   And the employee count: SWENo_Count
"""
import os
import pandas as pd
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


def pivotGen():
    """
    Generates a pivot table based on data from CSV file and exports it to an Excel file.
    Reads the CSV file "merged_modified.csv" located in the work_path.flatfile_folder directory.
    Extracts the relevant columns: "YearCensus", "LEAName", "Gender", "Ethnicity_Compact", "FTE",
    and "SWENo". Groups the data by "YearCensus", "LEAName", "Gender", and "Ethnicity_Compact",
    and calculates the sum of "FTE" values and the count of "SWENo" values for each group.
    Creates a pivot table using the calculated sums with column "FTESum" 
    and counts with column "SWENo_Count".
    Saves the resulting pivot table to an Excel file named "pivotTable.xlsx"
    in the work_path.request directory
    """
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
