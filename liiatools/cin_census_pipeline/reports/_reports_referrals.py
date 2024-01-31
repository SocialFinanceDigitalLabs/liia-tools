import numpy as np
import pandas as pd

from liiatools.cin_census_pipeline.spec import load_reports
from liiatools.cin_census_pipeline.reports import _time_between_date_series, _filter_events


def referral_outcomes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add referral outcomes to the data based on assessment and S47 dates. These can be;
    NFA, S17, S47 or BOTH

    :param data: The data calculate referral outcomes.
    :returns: The data with referral outcomes attached.
    """
    reports_config = load_reports()

    s17_dates = data[data["AssessmentActualStartDate"].notna()][
        ["LAchildID", "CINreferralDate", "AssessmentActualStartDate"]
    ].drop_duplicates()

    s17_dates["days_to_s17"] = _time_between_date_series(
        s17_dates["CINreferralDate"], s17_dates["AssessmentActualStartDate"], days=True
    )

    # Only assessments within config-specified period following referral are valid
    s17_dates = _filter_events(
        s17_dates, "days_to_s17", max_days=reports_config["ref_assessment"]
    )

    s47_dates = data[data["S47ActualStartDate"].notna()][
        ["LAchildID", "CINreferralDate", "S47ActualStartDate"]
    ].drop_duplicates()

    s47_dates["days_to_s47"] = _time_between_date_series(
        s47_dates["CINreferralDate"], s47_dates["S47ActualStartDate"], days=True
    )

    # Only S47s within config-specified period following referral are valid
    s47_dates = _filter_events(
        s47_dates, "days_to_s47", max_days=reports_config["ref_assessment"]
    )

    merged = data[["LAchildID", "CINreferralDate", "PersonBirthDate"]].drop_duplicates()
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

    merged["Age at referral"] = _time_between_date_series(
        merged["CINreferralDate"], merged["PersonBirthDate"], years=True
    )

    return merged
