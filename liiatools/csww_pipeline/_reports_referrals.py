import numpy as np
import pandas as pd


def referral_outcomes(data: pd.DataFrame) -> pd.DataFrame:
    s17_dates = data[data["AssessmentActualStartDate"].notna()][
        ["LAchildID", "CINreferralDate", "AssessmentActualStartDate"]
    ].drop_duplicates()
    s17_dates["days_to_s17"] = (
        s17_dates["CINreferralDate"] - s17_dates["AssessmentActualStartDate"]
    )
    s17_dates["days_to_s17"] = s17_dates["days_to_s17"].dt.days

    # Remove any that are less than zero - it shouldn't happen, but just in case
    s17_dates = s17_dates[s17_dates["days_to_s17"] >= 0]

    s47_dates = data[data["S47ActualStartDate"].notna()][
        ["LAchildID", "CINreferralDate", "S47ActualStartDate"]
    ].drop_duplicates()
    s47_dates["days_to_s47"] = (
        s47_dates["CINreferralDate"] - s47_dates["S47ActualStartDate"]
    )
    s47_dates["days_to_s47"] = s47_dates["days_to_s47"].dt.days

    # Remove any that are less than zero - it shouldn't happen, but just in case
    s47_dates = s47_dates[s47_dates["days_to_s47"] >= 0]

    merged = data[["LAchildID", "CINreferralDate"]].drop_duplicates()
    merged = merged.merge(s17_dates, how="left", on=["LAchildID", "CINreferralDate"])
    merged = merged.merge(s47_dates, how="left", on=["LAchildID", "CINreferralDate"])

    neither = (
        merged["AssessmentActualStartDate"].isna() & merged["S47ActualStartDate"].isna()
    )
    s17_set = (
        merged["AssessmentActualStartDate"].notna()
        & merged["S47ActualStartDate"].isna()
    )
    s47_set = (
        merged["AssessmentActualStartDate"].isna()
        & merged["S47ActualStartDate"].notna()
    )
    both_set = (
        merged["AssessmentActualStartDate"].notna()
        & merged["S47ActualStartDate"].notna()
    )

    merged["referral_outcome"] = np.select(
        [neither, s17_set, s47_set, both_set],
        ["NFA", "S17", "S47", "BOTH"],
        default=None,
    )

    return merged
