import pandas as pd

from liiatools.ssda903_pipeline.lds_ssda903_pan_agg import process


def test_match_load_file():
    test_df_1 = pd.DataFrame({"Column 1": [], "Column 2": []})
    test_df_2 = pd.DataFrame({"Column 3": [], "Column 4": []})
    column_names = {
        "Match 1": ["Column 1", "Column 2"],
        "Match 2": ["Column 3", "Column 4"],
    }
    table_name_1 = process.match_load_file(test_df_1, column_names)
    assert table_name_1 == "Match 1"
    table_name_2 = process.match_load_file(test_df_2, column_names)
    assert table_name_2 == "Match 2"


def test_merge_dfs():
    new_df_1 = pd.DataFrame({"LA": ["a"]})
    old_df = pd.DataFrame({"LA": ["b"]})
    assert_df = pd.DataFrame({"LA": ["a", "b"]})
    output_1 = process._merge_dfs(new_df_1, old_df, "a")
    assert output_1.equals(assert_df)
    new_df_2 = pd.DataFrame({"LA": ["b"]})
    output_2 = process._merge_dfs(new_df_2, old_df, "b")
    assert output_2.equals(new_df_2)
    assert output_2.equals(assert_df) == False
