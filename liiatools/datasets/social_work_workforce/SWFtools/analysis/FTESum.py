import os
import pandas as pd
import liiatools.datasets.social_work_workforce.SWFtools.util.work_path as work_path


def FTESum():
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
