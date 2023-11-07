from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np
import logging

log = logging.getLogger(__name__)


def read_file(input, dates):
    """
    Reads the csv file as a pandas DataFrame
    """
    flatfile = pd.read_csv(input, parse_dates=dates, dayfirst=True)
    return flatfile


def merge_la_files(flat_output, dates, flatfile):
    """
    Looks for existing file of the same type and merges with new file if found
    """
    old_file = Path(flat_output, f"CIN_Census_merged_flatfile.csv")
    if old_file.is_file():
        old_df = pd.read_csv(old_file, parse_dates=dates, dayfirst=True)
        merged_df = pd.concat([flatfile, old_df], axis=0)
    else:
        merged_df = flatfile
    return merged_df


def deduplicate(flatfile, sort_order, dedup):
    """
    Sorts and removes duplicate records from merged files following schema
    """
    flatfile = flatfile.sort_values(sort_order, ascending=False, ignore_index=True)
    flatfile = flatfile.drop_duplicates(subset=dedup, keep="first")
    return flatfile


def remove_old_data(flatfile, years):
    """
    Removes data older than a specified number of years
    """
    year = pd.to_datetime("today").year
    month = pd.to_datetime("today").month
    if month <= 6:
        year = year - 1
    flatfile = flatfile[flatfile["YEAR"] >= year - years]
    return flatfile


def export_flatfile(flat_output, flatfile):
    """
    Writes the flatfile output as a csv
    """
    output_path = Path(flat_output, f"CIN_Census_merged_flatfile.csv")
    flatfile.to_csv(output_path, index=False)


def filter_flatfile(flatfile, filter):
    """
    Filters rows to specified events
    Removes redundant columns that relate to other types of event
    """
    filtered_flatfile = flatfile[flatfile["Type"] == filter]
    filtered_flatfile = filtered_flatfile.dropna(axis=1, how="all")
    return filtered_flatfile


def split_factors(factors):
    """
    Creates a new set of columns from the flatfile with a column for each assessment factor
    Rows correspond the the rows of the flatfile and should have a value of 0 or 1 for each column
    """
    factor_cols = factors.Factors
    factor_cols = factor_cols.str.split(",", expand=True)
    factor_cols = factor_cols.stack()
    factor_cols = factor_cols.str.get_dummies()
    factor_cols = factor_cols.groupby(level=0).sum()
    assert factor_cols.isin([0, 1]).all(axis=None)
    factors = pd.concat([factors, factor_cols], axis=1)
    return factors


def export_factfile(analysis_output, factors):
    """
    Writes the factors output as a csv
    """
    output_path = Path(analysis_output, f"CIN_Census_factors.csv")
    factors.to_csv(output_path, index=False)


def referral_inputs(flatfile):
    """
    Creates three inputs for referral journeys analysis file
    """
    ref = filter_flatfile(flatfile, filter="CINreferralDate")
    s17 = filter_flatfile(flatfile, filter="AssessmentActualStartDate")
    s47 = filter_flatfile(flatfile, filter="S47ActualStartDate")
    return ref, s17, s47


# FIXME: This function defaults to returning nothing - should probably improve arguments to be a bit more helpful
#        or simply just returning the series and then doing `series // 365` in the calling function
def _time_between_date_series(later_date_series, earlier_date_series, years=0, days=0):
    days_series = later_date_series - earlier_date_series
    days_series = days_series.dt.days

    if days == 1:
        return days_series

    elif years == 1:
        years_series = (days_series / 365).apply(np.floor)
        years_series = years_series.astype("Int32")
        return years_series


def _filter_event_series(dataset: pd.DataFrame, days_series: str, max_days: int) -> pd.DataFrame:
    """
    Filters a dataframe to only include rows where the column named `days_series`
    is 0 <= days_series <= max_days

    Args:
        dataset (pd.DataFrame): The dataset to filter
        days_series (str): The name of the column containing the days between two events
        max_days (int): The maximum number of days between the two events
    
    Returns:
        pd.DataFrame: The filtered dataset

    """
    dataset = dataset[
        ((dataset[days_series] <= max_days) & (dataset[days_series] >= 0))
    ]
    return dataset


def merge_ref_s17(ref, s17, ref_assessment):
    """
    Merges ref and s17 views together, keeping only logically valid matches
    """
    # Merges referrals and assessments
    ref_s17 = ref.merge(
        s17[["LAchildID", "AssessmentActualStartDate"]], how="left", on="LAchildID"
    )

    # Calculates days between assessment and referral
    ref_s17["days_to_s17"] = _time_between_date_series(
        ref_s17["AssessmentActualStartDate"], ref_s17["CINreferralDate"], days=1
    )

    # Only assessments within config-specifed period following referral are valid
    ref_s17 = _filter_event_series(ref_s17, "days_to_s17", ref_assessment)

    # Reduces dataset to fields required for analysis
    ref_s17 = ref_s17[["Date", "LAchildID", "AssessmentActualStartDate", "days_to_s17"]]

    return ref_s17


def merge_ref_s47(ref, s47, ref_assessment):
    """
    Merges ref and s47 views together, keeping only logically valid matches
    """
    # Merges referrals and S47s
    ref_s47 = ref.merge(
        s47[["LAchildID", "S47ActualStartDate"]], how="left", on="LAchildID"
    )

    # Calculates days between S47 and referral
    ref_s47["days_to_s47"] = _time_between_date_series(
        ref_s47["S47ActualStartDate"], ref_s47["CINreferralDate"], days=1
    )

    # Only S47s within config-specifed period following referral are valid
    ref_s47 = _filter_event_series(ref_s47, "days_to_s47", ref_assessment)

    # Reduces dataset to fields required for analysis
    ref_s47 = ref_s47[["Date", "LAchildID", "S47ActualStartDate", "days_to_s47"]]

    return ref_s47


def ref_outcomes(ref, ref_s17, ref_s47):
    """
    Merges views together to give all outcomes of referrals in one place
    Outcomes column defaults to NFA unless there is a relevant S17 or S47 event to match
    Calculates age of child at referral
    """
    # Merge databases together
    ref_outs = ref.merge(ref_s17, on=["Date", "LAchildID"], how="left")
    ref_outs = ref_outs.merge(ref_s47, on=["Date", "LAchildID"], how="left")

    # Set default outcome to "NFA"
    ref_outs["referral_outcome"] = "NFA"

    # Set outcome to "S17" when there is a relevant assessment
    ref_outs.loc[
        ref_outs["AssessmentActualStartDate"].notnull(), "referral_outcome"
    ] = "S17"

    # Set outcome to "S47" when there is a relevant S47
    ref_outs.loc[ref_outs["S47ActualStartDate"].notnull(), "referral_outcome"] = "S47"

    # Set outcome to "Both S17 & S47" when there are both
    ref_outs.loc[
        (
            (ref_outs["AssessmentActualStartDate"].notnull())
            & (ref_outs["S47ActualStartDate"].notnull())
        ),
        "referral_outcome",
    ] = "Both S17 & S47"

    # Calculate age of child at referral
    ref_outs["Age at referral"] = _time_between_date_series(
        ref_outs["CINreferralDate"], ref_outs["PersonBirthDate"], years=1
    )

    return ref_outs


def export_reffile(analysis_output, ref_outs):
    """
    Writes the referral journeys output as a csv
    """
    output_path = Path(analysis_output, f"CIN_Census_referrals.csv")
    ref_outs.to_csv(output_path, index=False)


def journey_inputs(flatfile):
    """
    Creates inputs for the journey analysis file
    """
    # Create inputs from flatfile and merge them
    s47_j = filter_flatfile(flatfile, "S47ActualStartDate")
    cpp = filter_flatfile(flatfile, "CPPstartDate")
    return s47_j, cpp


def journey_merge(s47_j, cpp, icpc_cpp_days, s47_cpp_days):
    """
    Merges inputs to produce outcomes file
    """
    s47_cpp = s47_j.merge(
        cpp[["LAchildID", "CPPstartDate"]], how="left", on="LAchildID"
    )

    # Calculate days from ICPC to CPP start
    s47_cpp["icpc_to_cpp"] = _time_between_date_series(
        s47_cpp["CPPstartDate"], s47_cpp["DateOfInitialCPC"], days=1
    )

    # Calculate days from S47 to CPP start
    s47_cpp["s47_to_cpp"] = _time_between_date_series(
        s47_cpp["CPPstartDate"], s47_cpp["S47ActualStartDate"], days=1
    )

    # Only keep logically consistent events (as defined in config variables)
    s47_cpp = s47_cpp[
        ((s47_cpp["icpc_to_cpp"] >= 0) & (s47_cpp["icpc_to_cpp"] <= icpc_cpp_days))
        | ((s47_cpp["s47_to_cpp"] >= 0) & (s47_cpp["s47_to_cpp"] <= s47_cpp_days))
    ]

    # Merge events back to S47_j view
    s47_outs = s47_j.merge(
        s47_cpp[["Date", "LAchildID", "CPPstartDate", "icpc_to_cpp", "s47_to_cpp"]],
        how="left",
        on=["Date", "LAchildID"],
    )

    return s47_outs


def s47_paths(s47_outs, s47_day_limit, icpc_day_limit):
    """
    Creates an output that can generate a Sankey diagram of outcomes from S47 events
    """
    # Dates used to define window for S47 events where outcome may not be known because CIN Census is too recent
    for y in s47_outs["YEAR"]:
        s47_outs["cin_census_close"] = datetime(int(y), 3, 31)
    s47_outs["s47_max_date"] = s47_outs["cin_census_close"] - pd.Timedelta(
        s47_day_limit
    )
    s47_outs["icpc_max_date"] = s47_outs["cin_census_close"] - pd.Timedelta(
        icpc_day_limit
    )

    # Setting the Sankey diagram source for S47 events
    step1 = s47_outs.copy()
    step1["Source"] = "S47 strategy discussion"

    # Setting the Sankey diagram destination for S47 events
    step1["Destination"] = np.nan

    step1.loc[step1["DateOfInitialCPC"].notnull(), "Destination"] = "ICPC"

    step1.loc[
        step1["DateOfInitialCPC"].isnull() & step1["CPPstartDate"].notnull(),
        "Destination",
    ] = "CPP start"

    step1.loc[
        (
            (step1["Destination"].isnull())
            & (step1["S47ActualStartDate"] >= step1["s47_max_date"])
        ),
        "Destination",
    ] = "TBD - S47 too recent"

    step1.loc[step1["Destination"].isnull(), "Destination"] = "No ICPC or CPP"

    # Setting the Sankey diagram source for ICPC events
    step2 = step1[step1["Destination"] == "ICPC"]
    step2["Source"] = "ICPC"

    # Setting the Sankey diagram destination for ICPC events
    step2["Destination"] = np.nan

    step2.loc[step2["CPPstartDate"].notnull(), "Destination"] = "CPP start"

    step2.loc[
        (
            (step2["Destination"].isnull())
            & (step2["DateOfInitialCPC"] >= step2["icpc_max_date"])
        ),
        "Destination",
    ] = "TBD - ICPC too recent"

    step2.loc[step2["Destination"].isnull(), "Destination"] = "No CPP"

    # Merge the steps together
    s47_journey = pd.concat([step1, step2])

    # Calculate age of child at S47
    s47_journey["Age at S47"] = _time_between_date_series(
        s47_journey["S47ActualStartDate"], s47_journey["PersonBirthDate"], years=1
    )

    return s47_journey


def export_journeyfile(analysis_output, s47_journey):
    """
    Writes the S47 journeys output as a csv
    """
    output_path = Path(analysis_output, f"CIN_Census_S47_journey.csv")
    s47_journey.to_csv(output_path, index=False)
