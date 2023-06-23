import pandas as pd
import datetime
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
    sort_order = {
        "Table_1": ["Column 2"],
        "Table_2": ["Column 1"],
        "Table_3": ["Column 2"],
    }
    table_name_1 = "Table_1"
    table_name_2 = "Table_2"
    table_name_3 = "Table_3"
    dedup = {"Table_1": ["Column 2"], "Table_2": ["Column 2"], "Table_3": ["Column 1"]}
    output_df_1 = process.deduplicate(test_df_1, table_name_1, sort_order, dedup)
    assert len(output_df_1) == 1
    assert output_df_1["Column 3"][0] == "Answer 1"
    output_df_2 = process.deduplicate(test_df_1, table_name_2, sort_order, dedup)
    assert len(output_df_2) == 1
    assert output_df_2["Column 3"][0] == "Answer 2"
    output_df_3 = process.deduplicate(test_df_1, table_name_3, sort_order, dedup)
    assert len(output_df_3) == 2


def test_remove_old_data():
    latest_data_year = 2022
    last_year = latest_data_year - 1
    two_ya = latest_data_year - 2
    three_ya = latest_data_year - 3
    four_ya = latest_data_year - 4
    five_ya = latest_data_year - 5
    six_ya = latest_data_year - 6
    test_df_1 = pd.DataFrame(
        {
            "YEAR": [
                latest_data_year,
                last_year,
                two_ya,
                three_ya,
                four_ya,
                five_ya,
                six_ya,
            ]
        }
    )
    output_df_1 = process.remove_old_data(
        s903_df=test_df_1,
        num_of_years=7,
        new_year_start_month=1,
        as_at_date=datetime.datetime(2023, 7, 15),
    )
    assert len(output_df_1) == 6
    output_df_2 = process.remove_old_data(
        s903_df=test_df_1,
        num_of_years=7,
        new_year_start_month=1,
        as_at_date=datetime.datetime(2024, 1, 15),
    )
    assert len(output_df_2) == 5
