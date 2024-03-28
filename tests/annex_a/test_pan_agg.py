import pandas as pd

from liiatools.datasets.annex_a.lds_annexa_pan_agg import (
    configuration,
    process,
)


def test_data_minimisation():
    config = configuration.Config()
    List_10 = {"Column 1": []}
    List_11 = {"Column 1": []}
    test_dict_1 = {"List 10": pd.DataFrame(List_10), "List 11": pd.DataFrame(List_11)}
    output_dict_1 = process.data_minimisation(test_dict_1, config["minimise"])
    assert len(output_dict_1) == 0
    List_3 = {
        "Allocated Worker": ["John"],
        "Allocated Team": ["Janitor"],
        "Other column": ["Other data"],
    }
    test_dict_2 = {"List 3": pd.DataFrame(List_3)}
    output_dict_2 = process.data_minimisation(test_dict_2, config["minimise"])
    output_df_2 = output_dict_2["List 3"]
    assert len(output_df_2.columns) == 1


def test_merge_dfs():
    new_df_1 = {"LA": ["a"]}
    new_dict_1 = {"List 1": pd.DataFrame(new_df_1)}
    old_df = {"LA": ["b"]}
    old_dict = {"List 1": pd.DataFrame(old_df)}
    assert_df = {"LA": ["a", "b"]}
    assert_dict = {"List 1": pd.DataFrame(assert_df)}
    output_1 = process._merge_dfs(new_dict_1, old_dict, "a")
    assert output_1["List 1"].equals(assert_dict["List 1"])
    new_df_2 = {"LA": ["b"]}
    new_dict_2 = {"List 1": pd.DataFrame(new_df_2)}
    output_2 = process._merge_dfs(new_dict_2, old_dict, "b")
    assert output_2["List 1"].equals(assert_dict["List 1"]) == False
