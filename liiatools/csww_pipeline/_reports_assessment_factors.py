import pandas as pd


def expanded_assessment_factors(
    data: pd.DataFrame, column_name="AssessmentFactor", prefix: str = ""
) -> pd.DataFrame:
    """
    Expects to receive a dataframe with a column named 'AssessmentFactor' containing a comma-separated list of values.

    Expands these values into a "one-hot" encoding of the values. Can optionally prefix the column names with a
    prefix string.
    """

    factors = data[[column_name]].copy()
    factors[column_name] = factors[column_name].str.split(",")
    factors = factors.explode(column_name)
    factors[column_name] = factors[column_name].str.strip()
    factors = factors[factors[column_name] != ""]
    factors = pd.get_dummies(
        factors, columns=[column_name], prefix=prefix, prefix_sep=""
    )
    factors_grouped = factors.groupby(factors.index).max()

    factors_merged = data.merge(
        factors_grouped, how="left", left_index=True, right_index=True
    )
    factors_merged[factors_grouped.columns] = (
        factors_merged[factors_grouped.columns].fillna(0).astype(int)
    )
    return factors_merged
