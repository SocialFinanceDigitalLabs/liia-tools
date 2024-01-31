import pandas as pd
import numpy as np
from datetime import date

from liiatools.cin_census_pipeline.reports.reports import (
    expanded_assessment_factors,
    referral_outcomes,
    _time_between_date_series,
    _filter_events,
    s47_journeys,
)


def test_assessment_factors():
    df = pd.DataFrame(
        [
            ["CHILD1", "A,B,C"],
            ["CHILD1", None],
            ["CHILD1", ""],
            ["CHILD2", "A"],
            ["CHILD3", "D,A,D"],
        ],
        columns=["LAchildID", "Factors"],
    )

    df = expanded_assessment_factors(df)

    assert list(df.A.values) == [1, 0, 0, 1, 1]
    assert list(df.B.values) == [1, 0, 0, 0, 0]
    assert list(df.C.values) == [1, 0, 0, 0, 0]
    assert list(df.D.values) == [0, 0, 0, 0, 1]


def test_time_between_date_series():
    test_df_1 = pd.DataFrame(
        [
            [date(2022, 1, 1), date(2021, 1, 1)],
            [date(2022, 1, 1), date(2020, 1, 1)],
        ],
        columns=["date_series_1", "date_series_2"],
    )

    output_series_1 = _time_between_date_series(
        test_df_1["date_series_1"], test_df_1["date_series_2"], years=True
    )
    assert list(output_series_1) == [1, 2]

    output_series_2 = _time_between_date_series(
        test_df_1["date_series_1"], test_df_1["date_series_2"], days=True
    )
    assert list(output_series_2) == [365, 731]


def test_filter_events():
    test_df_1 = pd.DataFrame(
        [
            1,
            -1,
            30,
        ],
        columns=["day_series"],
    )

    output_1 = _filter_events(test_df_1, "day_series", 25)
    output_2 = _filter_events(test_df_1, "day_series", 30)
    assert output_1.shape == (1, 1)
    assert output_2.shape == (2, 1)


def test_referral_outcomes():
    df = pd.DataFrame(
        [
            [
                "CHILD1",
                date(1965, 6, 15),
                date(1970, 10, 6),
                date(1970, 6, 3),
                date(1970, 6, 2),
            ],
            [
                "CHILD1",
                date(1965, 6, 15),
                date(1970, 10, 6),
                date(1970, 6, 3),
                date(1970, 6, 2),
            ],
            [
                "CHILD2",
                date(1992, 1, 2),
                date(2001, 11, 7),
                date(2001, 10, 25),
                date(2001, 10, 20),
            ],
            [
                "CHILD3",
                date(1995, 7, 21),
                date(2003, 9, 5),
                date(2003, 8, 28),
                date(2003, 8, 26),
            ],
        ],
        columns=[
            "LAchildID",
            "PersonBirthDate",
            "CINreferralDate",
            "AssessmentActualStartDate",
            "S47ActualStartDate",
        ],
    )

    df = referral_outcomes(df)

    assert list(df["AssessmentActualStartDate"]) == [
        np.nan,
        date(2001, 10, 25),
        date(2003, 8, 28),
    ]
    assert list(df["days_to_s17"]) == [pd.NA, 13, 8]
    assert list(df["S47ActualStartDate"]) == [
        np.nan,
        date(2001, 10, 20),
        date(2003, 8, 26),
    ]
    assert list(df["days_to_s47"]) == [pd.NA, 18, 10]
    assert list(df["referral_outcome"]) == ["NFA", "BOTH", "BOTH"]
    assert list(df["Age at referral"]) == [5, 9, 8]


def test_s47_journeys():
    df = pd.DataFrame(
        [
            [
                "CHILD1",
                date(1965, 6, 15),
                date(1970, 10, 6),
                date(1970, 3, 3),
                date(1970, 5, 25),
                date(1970, 4, 5),
                2022,
            ],
            [
                "CHILD1",
                date(1965, 6, 15),
                date(1970, 10, 6),
                date(1970, 3, 3),
                date(1970, 5, 25),
                date(1970, 4, 5),
                2022,
            ],
            [
                "CHILD2",
                date(1992, 1, 2),
                date(2001, 11, 7),
                date(2001, 8, 2),
                date(2001, 10, 12),
                date(2001, 9, 29),
                2022,
            ],
            [
                "CHILD3",
                date(2015, 7, 21),
                date(2022, 9, 5),
                date(2022, 7, 27),
                pd.NA,
                pd.NA,
                2022,
            ],
            [
                "CHILD4",
                date(1997, 7, 21),
                date(2006, 9, 5),
                date(2006, 7, 28),
                date(2006, 8, 16),
                pd.NA,
                2022,
            ],
            [
                "CHILD5",
                date(1993, 4, 22),
                date(2001, 9, 2),
                date(2001, 7, 22),
                pd.NA,
                pd.NA,
                2022,
            ],
        ],
        columns=[
            "LAchildID",
            "PersonBirthDate",
            "CINreferralDate",
            "S47ActualStartDate",
            "CPPstartDate",
            "DateOfInitialCPC",
            "YEAR",
        ],
    )

    df = s47_journeys(df)

    assert list(df["icpc_to_cpp"]) == [13, pd.NA, 13]
    assert list(df["s47_to_cpp"]) == [71, 19, 71]
    assert list(df["cin_census_close"]) == [date(2022, 3, 31), date(2022, 3, 31), date(2022, 3, 31)]
    assert list(df["s47_max_date"]) == [date(2022, 1, 30), date(2022, 1, 30), date(2022, 1, 30)]
    assert list(df["icpc_max_date"]) == [date(2022, 2, 14), date(2022, 2, 14), date(2022, 2, 14)]
    assert list(df["Source"]) == ["S47 strategy discussion", "S47 strategy discussion", "ICPC"]
    assert list(df["Destination"]) == ["ICPC", "CPP Start", "CPP Start"]
    assert list(df["Age at S47"]) == [9, 9, 9]
