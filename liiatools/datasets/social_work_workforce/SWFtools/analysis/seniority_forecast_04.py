import pandas as pd
import numpy as np
import os
import work_path


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
