import pandas as pd

from liiatools.cin_census_pipeline.reports import expanded_assessment_factors


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

    print(df)
