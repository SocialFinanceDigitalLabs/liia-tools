import os
import pandas as pd
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path
import liiatools.datasets.social_work_workforce.SWFtools.util.AppLogs as AppLogs


def FTESum():
    """
    Calculate the sum of FTE by LEAName, YearCensus, SeniorityCode and SeniorityName from
    the input csv file

    :return: Excel file with the name FTESum_5d.xlsx and the same path as the input file
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
    Read a CSV file and calculate the sum of FTE by LEAName, YearCensus, SeniorityCode and
    SeniorityName for the year 2020

    :return: Excel file with the name FTESum_2020.xlsx and the same path as the input file
    """
    
    # ===== Read file ===== #
    file = "CompMergSen.csv"
    requestPath = work_path.request
    pathFile = os.path.join(requestPath, file)
    df = pd.read_csv(pathFile)

    df2020 = df[df["YearCensus"] == 2020]
    
    if df2020.empty:
        AppLogs.log("FTESum_2020 error: No data for year 2020", console_output=True)
    else:
        df5D = df2020[["LEAName", "YearCensus", "SeniorityCode", "SeniorityName", "FTE"]]

        df5D = df2020.groupby(
            ["LEAName", "YearCensus", "SeniorityCode", "SeniorityName"]
        ).agg(FTESum=("FTE", "sum"))

        # ===== Save and export file ===== #
        fileOutN = "FTESum_2020.xlsx"
        requestPath = work_path.request
        fileOut = os.path.join(requestPath, fileOutN)
        df5D.to_excel(fileOut, merge_cells=False)
