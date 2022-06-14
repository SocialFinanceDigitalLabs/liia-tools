import pandas as pd

from liiatools.datasets.annex_a.lds_annexa_la_agg import (
    configuration,
    process,
)

def test_sort_dict():
    sort_order = configuration.Config()['sort_order']
    df_right = {"Child Unique ID": [], "Gender": [], "Ethnicity": [], "Date of Birth": [], "Age of Child (Years)": [], "Date of Contact": [], "Contact Source": [], "LA": []}
    df_wrong = {"LA": [], "Child Unique ID": [], "Gender": [], "Ethnicity": [], "Date of Birth": [], "Age of Child (Years)": [], "Date of Contact": [], "Contact Source": []}
    dict_right = {"List 1": pd.DataFrame(df_right)}
    dict_wrong = {"List 1": pd.DataFrame(df_wrong)}
    output_1 = process.sort_dict(dict_right, sort_order)
    assert output_1["List 1"].equals(dict_right["List 1"])
    output_2 = process.sort_dict(dict_wrong, sort_order)
    assert output_2["List 1"].equals(dict_right["List 1"])


def test_merge_dfs():
    new_df = {"Column 1": ["a"]}
    new_dict = {"List 2": pd.DataFrame(new_df)}
    old_df = {"Column 1": ["b"]}
    old_dict = {"List 2": pd.DataFrame(old_df)}
    assert_df = {"Column 1": ["a", "b"]}
    assert_dict = {"List 2": pd.DataFrame(assert_df)}
    output_1 = process._merge_dfs(new_dict, old_dict)
    assert output_1["List 2"].equals(assert_dict["List 2"])


def test_deduplicate():
    new_df = {"Column 1": ["a", "b"], "Column 2": ["a", "a"]}
    new_dict = {"List": pd.DataFrame(new_df)}
    dedup_1 = {"List": ["Column 1"]}
    dedup_2 = {"List": ["Column 2"]}
    dedup_3 = {"List": ["Column 1", "Column 2"]}
    dedup_4 = {"List": ["Column 1"], "Other_list": ["Column 2"]}
    short_df = {"Column 1": ["a"], "Column 2": ["a"]}
    short_dict = {"List": pd.DataFrame(short_df)}
    output_1 = process.deduplicate(new_dict, dedup_1)
    assert output_1["List"].equals(new_dict["List"])
    output_2 = process.deduplicate(new_dict, dedup_2)
    assert output_2["List"].equals(short_dict["List"])
    output_3 = process.deduplicate(new_dict, dedup_3)
    assert output_3["List"].equals(new_dict["List"])
    output_4 = process.deduplicate(new_dict, dedup_4)
    assert output_4["List"].equals(new_dict["List"])


def test_remove_old_data():
    today = pd.to_datetime('today')
    five_years_ago = process._remove_years(today, 5)
    seven_years_ago = process._remove_years(today, 7)
    thirty_one_years_ago = process._remove_years(today, 31)
    test_df_1 = pd.DataFrame({"Date 1": [thirty_one_years_ago]})
    test_df_1["Date 1"] = pd.to_datetime(test_df_1["Date 1"], format="%d/%m/%Y")
    test_dict_1 = {"List 9": test_df_1}
    index_date_1 = {"List 9": "Date 1"}
    output_dict_1 = process.remove_old_data(test_dict_1, index_date_1)
    output_df_1 = output_dict_1["List 9"]
    assert output_df_1.shape[0] == 0
    test_df_2 = pd.DataFrame({"Date 1": [seven_years_ago]})
    test_df_2["Date 1"] = pd.to_datetime(test_df_2["Date 1"], format="%d/%m/%Y")
    test_dict_2 = {"List 9": test_df_2}
    output_dict_2 = process.remove_old_data(test_dict_2, index_date_1)
    output_df_2 = output_dict_2["List 9"]
    assert output_df_2.shape[0] == 1
    test_df_3 = pd.DataFrame({"Date 1": [seven_years_ago, seven_years_ago], "Date 2": [five_years_ago, seven_years_ago]})
    test_df_3["Date 1"] = pd.to_datetime(test_df_3["Date 1"], format="%d/%m/%Y")
    test_dict_3 = {"List 10": test_df_3}
    index_date_2 = {"List 10": ["Date 1", "Date 2"]}
    output_dict_3 = process.remove_old_data(test_dict_3, index_date_2)
    output_df_3 = output_dict_3["List 10"]
    assert output_df_3.shape[0] == 1
    test_df_4 = pd.DataFrame({"Date 1": [seven_years_ago, five_years_ago, None]})
    test_df_4["Date 1"] = pd.to_datetime(test_df_4["Date 1"], format="%d/%m/%Y")
    test_dict_4 = {"List 1": test_df_4}
    index_date_3 = {"List 1": "Date 1"}
    output_dict_4 = process.remove_old_data(test_dict_4, index_date_3)
    output_df_4 = output_dict_4["List 1"]
    assert output_df_4.shape[0] == 2
    test_df_5 = pd.DataFrame({"Date 1": [None, None, None]})
    test_df_5["Date 1"] = pd.to_datetime(test_df_5["Date 1"], format="%d/%m/%Y")
    test_dict_5 = {"List 1": test_df_5}
    output_dict_5 = process.remove_old_data(test_dict_5, index_date_3)
    output_df_5 = output_dict_5["List 1"]
    assert output_df_5.shape[0] == 3
    test_dict_6 = {"List 9": test_df_5}
    output_dict_6 = process.remove_old_data(test_dict_6, index_date_1)
    output_df_6 = output_dict_6["List 9"]
    assert output_df_6.shape[0] == 0