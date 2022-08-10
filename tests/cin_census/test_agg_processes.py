import pandas as pd
from liiatools.datasets.cin_census.lds_cin_la_agg import process as agg_process


def test_deduplicate():
    test_df_1 = pd.DataFrame(
        {
            "Column 1": [1, 2],
            "Column 2": ["a", "a"],
            "Column 3": ["Answer 1", "Answer 2"],
        }
    )
    sort_order = ["Column 1"]
    dedup_1 = ["Column 2"]
    output_df_1 = agg_process.deduplicate(test_df_1, sort_order, dedup_1)
    assert len(output_df_1) == 1
    assert output_df_1["Column 3"][0] == "Answer 2"
    dedup_2 = ["Column 1", "Column 2"]
    output_df_2 = agg_process.deduplicate(test_df_1, sort_order, dedup_2)
    assert len(output_df_2) == 2
    assert output_df_2["Column 3"][0] == "Answer 2"
    sort_order_2 = ["Column 2"]
    output_df_3 = agg_process.deduplicate(test_df_1, sort_order_2, dedup_2)
    assert len(output_df_3) == 2
    assert output_df_3["Column 3"][0] == "Answer 1"


def test_remove_old_data():
    this_year = pd.to_datetime("today").year
    month = pd.to_datetime("today").month
    last_year = this_year - 1
    two_ya = this_year - 2
    three_ya = this_year - 3
    test_df_1 = pd.DataFrame({"YEAR": [this_year, last_year, two_ya, three_ya]})
    output_df_1 = agg_process.remove_old_data(test_df_1, 1)
    if month <= 6:
        assert len(output_df_1) == 3
    else:
        assert len(output_df_1) == 2


def test_filter_flatfile():
    test_df_1 = pd.DataFrame(
        {
            "Type": ["Type 1", "Type 2"],
            "Type 1 data": ["a", None],
            "Type 2 data": [None, "b"],
        }
    )
    output_1 = agg_process.filter_flatfile(test_df_1, "Type 1")
    assert len(output_1) == 1
    assert output_1["Type 1 data"][0] == "a"
    output_2 = agg_process.filter_flatfile(test_df_1, "Type 2")
    assert len(output_2) == 1
    assert output_2["Type 2 data"][1] == "b"


def test_split_factors():
    test_df_1 = pd.DataFrame({"Factors": ["a,b", "b,c"]})
    assert test_df_1.shape == (2, 1)
    assert list(test_df_1.columns) == ["Factors"]
    assert test_df_1.iloc[0, 0] == "a,b"
    output_1 = agg_process.split_factors(test_df_1)
    assert output_1.shape == (2, 4)
    assert list(output_1.columns) == ["Factors", "a", "b", "c"]
    assert output_1.iloc[0, 1] == 1


def test_time_between_date_series():
    test_df_1 = pd.DataFrame(
        {
            "date_series_1": ["2022-01-01", "2022-01-01"],
            "date_series_2": ["2021-01-01", "2020-01-01"],
        },
        dtype="datetime64[ns]",
    )
    output_series_1 = agg_process._time_between_date_series(
        test_df_1["date_series_1"], test_df_1["date_series_2"], years=1
    )
    assert list(output_series_1) == [1, 2]
    output_series_2 = agg_process._time_between_date_series(
        test_df_1["date_series_1"], test_df_1["date_series_2"], days=1
    )
    assert list(output_series_2) == [365, 731]


def test_filter_event_series():
    test_df_1 = pd.DataFrame({"day_series": [1, -1, 30]})
    output_1 = agg_process._filter_event_series(test_df_1, "day_series", 25)
    output_2 = agg_process._filter_event_series(test_df_1, "day_series", 30)
    assert output_1.shape == (1, 1)
    assert output_2.shape == (2, 1)
