import pandas as pd
import numpy as np
from datetime import datetime

from liiatools.cin_census_pipeline.spec import load_reports
from liiatools.cin_census_pipeline.reports import (
    _time_between_date_series,
)


def s47_journeys(data: pd.DataFrame) -> pd.DataFrame:
    """
    Creates an output that can generate a Sankey diagram of outcomes from S47 events

    :param data: The data to calculate S47 event outcomes.
    :return: The data with S47 outcomes attached.
    """
    reports_config = load_reports()

    s47_dates = data[data["S47ActualStartDate"].notna()][
        ["LAchildID", "CINreferralDate", "S47ActualStartDate"]
    ].drop_duplicates()

    cpp_dates = data[data["CPPstartDate"].notna()][
        ["LAchildID", "CINreferralDate", "CPPstartDate"]
    ].drop_duplicates()

    merged = data[
        [
            "LAchildID",
            "CINreferralDate",
            "PersonBirthDate",
            "DateOfInitialCPC",
            "Year",
        ]
    ].drop_duplicates()

    merged = merged.merge(s47_dates, how="left", on=["LAchildID", "CINreferralDate"])
    merged = merged.merge(cpp_dates, how="left", on=["LAchildID", "CINreferralDate"])

    merged["icpc_to_cpp"] = _time_between_date_series(
        merged["CPPstartDate"], merged["DateOfInitialCPC"], days=True
    )

    merged["s47_to_cpp"] = _time_between_date_series(
        merged["CPPstartDate"], merged["S47ActualStartDate"], days=True
    )

    # Only keep logically consistent events (as defined in config variables)
    merged = merged[
        (
            (merged["icpc_to_cpp"] >= 0)
            & (merged["icpc_to_cpp"] <= reports_config["icpc_cpp_days"])
        )
        | (
            (merged["s47_to_cpp"] >= 0)
            & (merged["s47_to_cpp"] <= reports_config["s47_cpp_days"])
        )
    ]

    # Dates used to define window for S47 events where outcome may not be known because CIN Census is too recent
    for y in merged["Year"]:
        merged["cin_census_close"] = datetime(int(y), 3, 31)

    merged["s47_max_date"] = merged["cin_census_close"] - pd.Timedelta(
        reports_config["s47_day_limit"]
    )
    merged["icpc_max_date"] = merged["cin_census_close"] - pd.Timedelta(
        reports_config["icpc_day_limit"]
    )

    merged["Source"] = "S47 strategy discussion"

    icpc = merged["DateOfInitialCPC"].notna()

    cpp_start = merged["DateOfInitialCPC"].isna() & merged["CPPstartDate"].notna()

    # TODO: Check if this (and the default=No ICPC or CPP) ever actually comes up
    #  (I think they're removed when checking for logical events)
    tbd = merged["S47ActualStartDate"] >= merged["s47_max_date"]

    merged["Destination"] = np.select(
        [icpc, cpp_start, tbd],
        ["ICPC", "CPP Start", "TBD - S47 too recent"],
        default="No ICPC or CPP",
    )

    icpc_destination = merged[merged["Destination"] == "ICPC"]
    icpc_destination["Source"] = "ICPC"

    cpp_start_2 = icpc_destination["CPPstartDate"].notna()

    tbd_2 = icpc_destination["DateOfInitialCPC"] >= icpc_destination["icpc_max_date"]

    icpc_destination["Destination"] = np.select(
        [cpp_start_2, tbd_2],
        ["CPP Start", "TBD - ICPC too recent"],
        default="No CPP",
    )

    s47_journey = pd.concat([merged, icpc_destination])

    s47_journey["Age at S47"] = _time_between_date_series(
        s47_journey["S47ActualStartDate"], s47_journey["PersonBirthDate"], years=True
    )

    return s47_journey
