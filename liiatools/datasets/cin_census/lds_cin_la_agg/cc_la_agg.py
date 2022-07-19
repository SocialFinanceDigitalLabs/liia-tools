from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np
import logging
import os

from liiatools.datasets.cin_census.lds_cin_la_agg import cc_la_agg_config

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

log = logging.getLogger(__name__)


def read_file(input):
    try:
        df = pd.read_csv(input, parse_dates=cc_la_agg_config.dates)
        return df
    except Exception as ex:
        log.exception(f"Error trying to split file: {input}")


def merge_la_files(flat_output, new_df, input):
    try:
        file_list = Path(flat_output).glob("*.csv")
        for file in file_list:
            old_df = pd.read_csv(file, parse_dates=cc_la_agg_config.dates)
            new_df = pd.concat([new_df, old_df], axis=0)
        return new_df
    except Exception as ex:
        log.exception(f"Error trying to merge local authority files for {input}")


def deduplicate(df, input):
    try:
        df = df.sort_values(cc_la_agg_config.sort_order, ascending=False)
        df = df.drop_duplicates(subset=cc_la_agg_config.dedup, keep="first")
        return df
    except Exception as ex:
        log.exception(f"Error trying to deduplicate files for {input}")


def remove_old_data(df, input):
    try:
        year = pd.to_datetime("today").year
        month = pd.to_datetime("today").month
        if month <= 6:
            year = year - 1
        df = df[df["YEAR"] >= year - 5]
        return df
    except Exception as ex:
        log.exception(f"Error trying to remove old data from {input}")


def log_missing_years(df, input, la_log_dir):
    """
    Output a log of the missing years for merged dataframes
    """
    try:
        expected_years = pd.Series(np.arange(df["YEAR"].min(), df["YEAR"].max() + 1))
        actual_years = df["YEAR"].unique()
        missing_years = expected_years[~expected_years.isin(actual_years)]
        clean_missing_years = str(missing_years.values)[
            1:-1
        ]  # Remove brackets from missing_years

        filename = Path(input).stem
        if clean_missing_years:
            with open(
                f"{os.path.join(la_log_dir, filename)}_error_log_{start_time}.txt", "a"
            ) as f:
                f.write(f"{filename}_{start_time}")
                f.write("\n")
                f.write(f"Years missing from dataset: {clean_missing_years}")
                f.write("\n")

        return df
    except Exception as ex:
        log.exception(f"Error occurred in {input}")


def export_flatfile(flat_output, df, input):
    try:
        output_path = Path(flat_output, f"CIN_Census_merged_flatfile.csv")
        df.to_csv(output_path, index=False)
    except Exception as ex:
        log.exception(f"Error trying to export files from {input}")


def factors_inputs(df):
    """Reduces rows to only assessment events
    Removes redundant columns that relate to other types of event"""
    df = df[df["Type"] == "AssessmentAuthorisationDate"]
    df = df.dropna(axis=1, how="all")
    return df


def split_factors(df):
    """Creates a new set of columns from the flatfile with a column for each assessment factor
    Rows correspond the the rows of the flatfile and should have a value of 0 or 1 for each column"""
    factor_cols = (
        df.Factors.str.split(",", expand=True).stack().str.get_dummies().sum(level=0)
    )
    assert factor_cols.isin([0, 1]).all().all()
    df = pd.concat([df, factor_cols], axis=1)
    return df


def export_factfile(analysis_output, df):
    """Writes the new assessment factors file as a csv in the LA's Analysis folder"""
    output_path = Path(analysis_output, f"CIN_Census_factors.csv")
    df.to_csv(output_path, index=False)


def referral_inputs(df):
    """Creates three views needed for generating referral journeys analysis file"""
    ref = df[df["Type"] == "CINreferralDate"]
    ref = ref.dropna(axis=1, how="all")
    s17 = df[df["Type"] == "AssessmentActualStartDate"]
    s17 = s17.dropna(axis=1, how="all")
    s47 = df[df["Type"] == "S47ActualStartDate"]
    s47 = s47.dropna(axis=1, how="all")
    return ref, s17, s47


def merge_ref_s17(ref, s17):
    """Merges ref and s17 views together, keeping only logically valid matches"""
    data_s17 = ref.merge(
        s17[["LAchildID", "AssessmentActualStartDate"]], how="left", on="LAchildID"
    )
    data_s17["days_to_s17"] = (
        data_s17["AssessmentActualStartDate"] - data_s17["CINreferralDate"]
    )
    data_s17["days_to_s17"] = data_s17["days_to_s17"].dt.days
    # Only s17 events after (>0) but within 30 days (<= ref_assessment) of referral events are valid
    ref_s17 = data_s17[
        (
            (data_s17["days_to_s17"] <= cc_la_agg_config.ref_assessment)
            & (data_s17["days_to_s17"] >= 0)
        )
    ]
    ref_s17 = ref_s17[["Date", "LAchildID", "AssessmentActualStartDate", "days_to_s17"]]
    return ref_s17


def merge_ref_s47(ref, s47):
    """Merges ref and s47 views together, keeping only logically valid matches"""
    data_s47 = ref.merge(
        s47[["LAchildID", "S47ActualStartDate"]], how="left", on="LAchildID"
    )
    data_s47["days_to_s47"] = (
        data_s47["S47ActualStartDate"] - data_s47["CINreferralDate"]
    )
    data_s47["days_to_s47"] = data_s47["days_to_s47"].dt.days
    # Only s47 events after (>0) but within 30 days (<= ref_assessment) of referral events are valid
    ref_s47 = data_s47[
        (
            (data_s47["days_to_s47"] <= cc_la_agg_config.ref_assessment)
            & (data_s47["days_to_s47"] >= 0)
        )
    ]
    ref_s47 = ref_s47[["Date", "LAchildID", "S47ActualStartDate", "days_to_s47"]]
    return ref_s47


def ref_outcomes(ref, ref_s17, ref_s47):
    """Merges views together as outcomes of referrals
    Outcomes column defaults to NFA unless there is a relevant S17 or S47 event to match
    Calculates age of child at referral"""
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
    ref_outs["Age at referral"] = (
        ref_outs["CINreferralDate"] - ref_outs["PersonBirthDate"]
    )
    ref_outs["Age at referral"] = ref_outs["Age at referral"].dt.days
    ref_outs["Age at referral"] = (ref_outs["Age at referral"] / 365.25).apply(np.floor)
    ref_outs["Age at referral"] = ref_outs["Age at referral"].astype(int)
    return ref_outs


def export_reffile(analysis_output, df):
    """Writes the new referral journeys file as a csv in the LA's Analysis folder"""
    output_path = Path(analysis_output, f"CIN_Census_referrals.csv")
    df.to_csv(output_path, index=False)


def journey_inputs(df):
    """Creates the input view for the journey analysis file"""
    s47_j = df[df["Type"] == "S47ActualStartDate"]
    s47_j = s47_j.dropna(axis=1, how="all")
    cpp = df[df["Type"] == "CPPstartDate"]
    cpp = cpp.dropna(axis=1, how="all")
    s47_cpp = s47_j.merge(
        cpp[["LAchildID", "CPPstartDate"]], how="left", on="LAchildID"
    )
    # Calculate days from ICPC to CPP start
    s47_cpp["icpc_to_cpp"] = s47_cpp["CPPstartDate"] - s47_cpp["DateOfInitialCPC"]
    s47_cpp["icpc_to_cpp"] = s47_cpp["icpc_to_cpp"].dt.days
    # Calculate days from S47 to CPP start
    s47_cpp["s47_to_cpp"] = s47_cpp["CPPstartDate"] - s47_cpp["S47ActualStartDate"]
    s47_cpp["s47_to_cpp"] = s47_cpp["s47_to_cpp"].dt.days
    # Only keep logically consistent events (as defined in config variables)
    s47_cpp = s47_cpp[
        (
            (s47_cpp["icpc_to_cpp"] >= 0)
            & (s47_cpp["icpc_to_cpp"] <= cc_la_agg_config.icpc_cpp_days)
        )
        | (
            (s47_cpp["s47_to_cpp"] >= 0)
            & (s47_cpp["s47_to_cpp"] <= cc_la_agg_config.s47_cpp_days)
        )
    ]
    # Merge events back to S47_j view
    s47_outs = s47_j.merge(
        s47_cpp[["Date", "LAchildID", "CPPstartDate", "icpc_to_cpp", "s47_to_cpp"]],
        how="left",
        on=["Date", "LAchildID"],
    )
    return s47_outs


def s47_paths(df):
    """Creates an output that can generate a Sankey diagram of outcomes from S47 events"""
    # Dates used to define window for S47 events where outcome may not be known because CIN Census is too recent
    df["cin_census_close"] = datetime(df["YEAR"], 3, 31)
    df["s47_max_date"] = df["cin_census_close"] - pd.Timedelta("60 days")
    df["icpc_max_date"] = df["cin_census_close"] - pd.Timedelta("45 days")
    # Setting the Sankey diagram source for S47 events
    step1 = df.copy()
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
    s47_journey["Age at S47"] = (
        s47_journey["S47ActualStartDate"] - s47_journey["PersonBirthDate"]
    )
    s47_journey["Age at S47"] = s47_journey["Age at S47"].dt.days
    s47_journey["Age at S47"] = (s47_journey["Age at S47"] / 365.25).apply(np.floor)
    s47_journey["Age at S47"] = s47_journey["Age at S47"].astype(int)
    return s47_journey


def export_journeyfile(analysis_output, df):
    """Writes the new S47 journeys file as a csv in the LA's Analysis folder"""
    output_path = Path(analysis_output, f"CIN_Census_S47_journey.csv")
    df.to_csv(output_path, index=False)
