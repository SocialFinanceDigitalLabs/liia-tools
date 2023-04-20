import os
import pandas as pd
import numpy as np
import work_path

'''
  Growth rate table and Population growth table: 2020 to 2026
'''


def growth_tables():

    growth_rate_df = {'LEAName': ['Havering', 'Barking and Dagenham', 'Redbridge', 'Newham', 'Tower Hamlets', 'Waltham Forest'],
                      '2020': [0.0118, 0.0121, 0.0129, 0.0195, 0.0144, 0.0197],
                      '2021': [0.3639, 0.0136, 0.0249, 0.0090, 0.0115, 0.0086],
                      '2022': [0.0309, 0.0111, 0.0247, 0.0117, 0.0107, 0.0100],
                      '2023': [0.0248, 0.0120, 0.0301, 0.0073, 0.0114, 0.0091],
                      '2024': [0.0254, 0.0137, 0.0261, 0.0118, 0.0115, 0.0087],
                      '2025': [0.0227, 0.0113, 0.0208, 0.0095, 0.0100, 0.0102],
                      '2026': [0.023, 0.0107, 0.0243, 0.0093, 0.0118, 0.0058]
                      }

    growth_rate_table = pd.DataFrame(growth_rate_df)

    # print(growth_rate_table)

    # ===== Save and export file ===== #
    fileOutN = 'growth_rate_table.xlsx'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    growth_rate_table.to_excel(fileOut, index=False)

    print("Auxiliary table: ", fileOutN, " Created")

    '''
      Population growth table: 2020 to 2026
    '''

    population_growth_df = {'LEAName': ['Barking and Dagenham', 'Havering', 'Newham', 'Redbridge', 'Tower Hamlets', 'Waltham Forest'],
                            '2020': [320128, 161409, 461239, 215230, 529399, 383747],
                            '2021': [324487, 220150, 465409, 220598, 535487, 387033],
                            '2022': [328104, 226948, 470857, 226048, 541209, 390895],
                            '2023': [332057, 232581, 474304, 232857, 547400, 394462],
                            '2024': [336590, 238498, 479905, 238942, 553698, 397877],
                            '2025': [340410, 243909, 484480, 243909, 559221, 401948],
                            '2026': [344040, 249520, 488977, 249844, 565820, 404285]
                            }

    population_growth_table = pd.DataFrame(population_growth_df)

    # print(population_growth_table)

    # ===== Save and export file ===== #
    fileOutN = 'population_growth_table.xlsx'
    requestPath = work_path.request
    fileOut = os.path.join(requestPath, fileOutN)
    population_growth_table.to_excel(fileOut, index=False)

    print("Auxiliary table: ", fileOutN, " Created")
