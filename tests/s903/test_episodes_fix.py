import pandas as pd

from liiatools.datasets.s903.lds_ssda903_episodes_fix.process import (
    create_previous_and_next_episode,
    add_latest_year_and_source_for_la,
    _is_next_episode_duplicate,
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
    assert data_with_previous_next_episode["DECOM_previous"].tolist() == [
        None,
        "2016-07-26",
        "2016-08-22",
    ]
    assert data_with_previous_next_episode["DECOM_next"].tolist() == [
        "2016-08-22",
        "2016-09-13",
        None,
    ]
    assert data_with_previous_next_episode["RNE_previous"].tolist() == [None, "S", "L"]
    assert data_with_previous_next_episode["RNE_next"].tolist() == ["L", "P", None]
    assert data_with_previous_next_episode["YEAR_previous"].tolist() == [
        None,
        2016,
        2016,
    ]
    assert data_with_previous_next_episode["YEAR_next"].tolist() == [2016, 2016, None]


def test_add_latest_year_and_source_for_la():
    data = pd.DataFrame(
        {
            "LA": ["BAD", "BAD", "NEW", "NEW"],
            "YEAR": [2019, 2020, 2022, 2021],
        }
    )

    data_with_latest_year_and_source_for_la = add_latest_year_and_source_for_la(data)
    assert data_with_latest_year_and_source_for_la["YEAR_latest"].tolist() == [
        2020,
        2020,
        2022,
        2022,
    ]
    assert data_with_latest_year_and_source_for_la["Episode_source"].tolist() == [
        "Original",
        "Original",
        "Original",
        "Original",
    ]


def test__is_next_episode_duplicate():
    data = pd.DataFrame(
        {
            "DEC": [None, None, None, None, None, None, None, None, None],
            "Has_next_episode": [True, True, True, True, True, True, True, True, True],
            "DECOM": [
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
                "2016-08-22",
            ],
            "DECOM_next": [
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
                "2016-11-22",
            ],
            "RNE": ["P", "P", "P", "P", "P", "P", "P", None, "P"],
            "RNE_next": ["P", "DIFF", "P", "P", "P", "P", "P", None, None],
            "LS": ["C2", "C2", "C2", "C2", "C2", "C2", "C2", None, "C2"],
            "LS_next": ["C2", "C2", "DIFF", "C2", "C2", "C2", "C2", None, None],
            "PLACE": ["U1", "U1", "U1", "U1", "U1", "U1", "U1", None, "U1"],
            "PLACE_next": ["U1", "U1", "U1", "DIFF", "U1", "U1", "U1", None, None],
            "PLACE_PROVIDER": [
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                None,
                "PR1",
            ],
            "PLACE_PROVIDER_next": [
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "DIFF",
                "PR1",
                "PR1",
                None,
                None,
            ],
            "PL_POST": [
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                None,
                "ABC1",
            ],
            "PL_POST_next": [
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "DIFF",
                "ABC1",
                None,
                None,
            ],
            "URN": [
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                None,
                "SC1234",
            ],
            "URN_next": [
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "DIFF",
                None,
                None,
            ],
        }
    )

    data["Test result"] = _is_next_episode_duplicate(data)
    assert data["Test result"].tolist() == [
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        True,
        False,
    ]
