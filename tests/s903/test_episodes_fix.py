import pandas as pd

from liiatools.datasets.s903.lds_ssda903_episodes_fix.process import (
    create_previous_and_next_episode,
)


def test_create_previous_and_next_episode():
    data = pd.DataFrame(
        {
            "CHILD": ["123", "123", "123"],
            "DECOM": ["2016-07-26", "2016-08-22", "2016-09-13"],
            "RNE": ["S", "L", "P"],
            "YEAR": [2016, 2016, 2016],
        }
    )

    columns = ["DECOM", "RNE", "YEAR"]

    data_with_previous_next_episode = create_previous_and_next_episode(data, columns)
    assert data_with_previous_next_episode["DECOM_previous"].tolist() == [None, "2016-07-26", "2016-08-22"]
    assert data_with_previous_next_episode["DECOM_next"].tolist() == ["2016-08-22", "2016-09-13", None]
    assert data_with_previous_next_episode["RNE_previous"].tolist() == [None, "S", "L"]
    assert data_with_previous_next_episode["RNE_next"].tolist() == ["L", "P", None]
    assert data_with_previous_next_episode["YEAR_previous"].tolist() == [None, 2016, 2016]
    assert data_with_previous_next_episode["YEAR_next"].tolist() == [2016, 2016, None]
