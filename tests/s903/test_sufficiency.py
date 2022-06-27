import pandas as pd

from liiatools.datasets.s903.lds_ssda893_sufficiency import process

def test_match_load_file():
    test_df_1 = pd.DataFrame({"Column 1": [], "Column 2": []})
    test_df_2 = pd.DataFrame({"Column 3": [], "Column 4": []})
    column_names = {"Match 1": ["Column 1", "Column 2"], "Match 2": ["Column 3", "Column 4"]}
    table_name_1 = process.match_load_file(test_df_1, column_names)
    assert table_name_1 == "Match 1"
    table_name_2 = process.match_load_file(test_df_2, column_names)
    assert table_name_2 == "Match 2"

def test_minimise():
    test_df_1 = pd.DataFrame({"Column 1" : [], "Column 2": []})
    table_name_1 = "dataset 1"
    minimise = {"dataset 1": ["Column 1"]}
    output_1 = process.data_min(test_df_1, minimise, table_name_1)
    assert output_1.shape[1] == 1
    table_name_2 = "dataset 2"
    output_2 = process.data_min(test_df_1, minimise, table_name_2)
    assert output_2.shape[1] == 2