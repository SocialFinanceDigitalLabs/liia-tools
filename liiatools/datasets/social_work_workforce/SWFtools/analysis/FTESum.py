import os
import pandas as pd
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


def FTESum():
    """
    Calculates the sum of FTE values from a CSV file and
    export the work_path.py result to an Excel file.
    Reads the CSV file named "CompMergSen.csv" located in work_path.request directory
    and extracts the columns: "LEAName", "YearCensus", "SeniorityCode",
    "SeniorityName", and "FTE". Sorts the values by "LEAName", "YearCensus", and
    "SeniorityCode".
    Groups the values by "LEAName", "YearCensus", "SeniorityCode", and "SeniorityName"
    and calculates the sum of FTE values for each group
    resulting in a new DataFrame with the "FTESum" column.
    Saves the resulting DataFrame to an Excel file
    named "FTESum_5d.xlsx" in work_path.request directory
    """
    # ===== Read file ===== #
    file = "CompMergSen.csv"
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    df = pd.read_csv(pathFile)

    df5C = df[["LEAName", "YearCensus", "SeniorityCode", "SeniorityName", "FTE"]]

    df5C = df5C.sort_values(by=["LEAName", "YearCensus", "SeniorityCode"])

    df5C = df.groupby(["LEAName", "YearCensus", "SeniorityCode", "SeniorityName"]).agg(
        FTESum=("FTE", "sum")
    )

    # ===== Save and export file ===== #
    fileOutN = "FTESum_5d.xlsx"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df5C.to_excel(fileOut, merge_cells=False)


def FTESum_2020():
    """
    Calculates the sum of FTE values for the year 2020 from a CSV file and 
    exports the result to an Excel file.
    Reads the CSV file named "CompMergSen.csv" located in the work_path.request directory
    and extracts the columns: "LEAName", "YearCensus", "SeniorityCode", "SeniorityName",
    and "FTE". Filters to only include only rows where "YearCensus" column is equal to 2020.
    Groups the filtered values by "LEAName", "YearCensus", "SeniorityCode", and "SeniorityName"
    and calculates the sum of FTE values for each group
    resulting in a new DataFrame with the "FTESum" column.
    Saves the resulting DataFrame to an Excel file named "FTESum_2020.xlsx"
    in the work_path.request directory
    """
    # ===== Read file ===== #
    file = "CompMergSen.csv"
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    df = pd.read_csv(pathFile)

    df2020 = df[df["YearCensus"] == 2020]

    df5D = df2020[["LEAName", "YearCensus", "SeniorityCode", "SeniorityName", "FTE"]]

    df5D = df2020.groupby(
        ["LEAName", "YearCensus", "SeniorityCode", "SeniorityName"]
    ).agg(FTESum=("FTE", "sum"))

    # ===== Save and export file ===== #
    fileOutN = "FTESum_2020.xlsx"
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    df5D.to_excel(fileOut, merge_cells=False)
