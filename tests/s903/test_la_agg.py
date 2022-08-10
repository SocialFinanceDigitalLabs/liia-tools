import pandas as pd
from liiatools.datasets.s903.lds_ssda903_la_agg import process


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


def test_deduplicate():
    test_df_1 = pd.DataFrame(
        {
            "Column 1": [1, 2],
            "Column 2": ["a", "a"],
            "Column 3": ["Answer 1", "Answer 2"],
        }
    )
    sort_order = {"Sorted": ["Column 1"]}
    table_name_1 = "Not_sorted"
    table_name_2 = "Sorted"
    table_name_3 = "Something else"
    dedup = {"Sorted": ["Column 2"], "Not_sorted": ["Column 2"]}
    output_df_1 = process.deduplicate(test_df_1, table_name_1, sort_order, dedup)
    assert len(output_df_1) == 1
    assert output_df_1["Column 3"][0] == "Answer 1"
    output_df_2 = process.deduplicate(test_df_1, table_name_2, sort_order, dedup)
    assert len(output_df_2) == 1
    assert output_df_2["Column 3"][0] == "Answer 2"
    output_df_3 = process.deduplicate(test_df_1, table_name_3, sort_order, dedup)
    assert len(output_df_3) == 2


def test_remove_old_data():
    this_year = pd.to_datetime("today").year
    month = pd.to_datetime("today").month
    last_year = this_year - 1
    two_ya = this_year - 2
    three_ya = this_year - 3
    test_df_1 = pd.DataFrame({"YEAR": [this_year, last_year, two_ya, three_ya]})
    output_df_1 = process.remove_old_data(test_df_1, 1)
    if month <= 6:
        assert len(output_df_1) == 3
    else:
        assert len(output_df_1) == 2
