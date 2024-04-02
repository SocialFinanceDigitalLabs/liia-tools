import pandas as pd

from liiatools.datasets.s903.lds_ssda903_episodes_fix.process import (
    create_previous_and_next_episode,
    add_latest_year_and_source_for_la,
    _is_the_same,
    _is_next_episode_duplicate,
    _is_previous_episode_duplicate,
    _is_previous_episode_submitted_later,
    _stage1_rule_to_apply,
    add_stage1_rule_identifier_columns,
    identify_stage1_rule_to_apply,
    _update_dec_stage1,
    _update_rec_stage1,
    _update_reason_place_change_stage1,
    _update_episode_source_stage1,
    apply_stage1_rules,
    _overlaps_next_episode,
    _has_x1_gap_before_next_episode,
    _stage2_rule_to_apply,
    _update_dec_stage2,
    _update_episode_source_stage2,
    add_stage2_rule_identifier_columns,
    identify_stage2_rule_to_apply,
    apply_stage2_rules,
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


def test_add_stage1_rule_identifier_columns():
    data = pd.DataFrame(
        {
            "DEC": [None, None],
            "YEAR": [2019, 2019],
            "YEAR_latest": [2022, 2019],
            "DECOM_next": ["2019-10-10", None],
            "RNE_next": ["S", None],
            # following required in Dataframe but not part of test
            "YEAR_previous": [None, None],
            "DECOM": [None, None],
            "DECOM_previous": [None, None],
            "RNE": [None, None],
            "RNE_previous": [None, None],
            "LS": [None, None],
            "LS_next": [None, None],
            "LS_previous": [None, None],
            "PLACE": [None, None],
            "PLACE_next": [None, None],
            "PLACE_previous": [None, None],
            "PLACE_PROVIDER": [None, None],
            "PLACE_PROVIDER_next": [None, None],
            "PLACE_PROVIDER_previous": [None, None],
            "PL_POST": [None, None],
            "PL_POST_next": [None, None],
            "PL_POST_previous": [None, None],
            "URN": [None, None],
            "URN_next": [None, None],
            "URN_previous": [None, None],
        }
    )
    data_with_identifiers_added = add_stage1_rule_identifier_columns(data)
    assert data_with_identifiers_added["Has_open_episode_error"].tolist() == [
        True,
        False,
    ]
    assert data_with_identifiers_added["Has_next_episode"].tolist() == [True, False]
    assert data_with_identifiers_added[
        "Has_next_episode_with_RNE_equals_S"
    ].tolist() == [True, False]


def test__is_the_same():
    data = pd.DataFrame(
        {
            "VALUE1": ["123", "123", "123", None],
            "VALUE2": ["123", "456", None, None],
        }
    )
    data["Test result"] = _is_the_same(data["VALUE1"], data["VALUE2"])
    assert data["Test result"].tolist() == [
        True,
        False,
        False,
        True,
    ]


def test__is_next_episode_duplicate():
    data = pd.DataFrame(
        {
            "DEC": [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "2016-08-31",
                None,
            ],
            "Has_next_episode": [
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                False,
            ],
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
                "2016-11-22",
                None,
            ],
            "RNE": ["P", "P", "P", "P", "P", "P", "P", None, "P", "P", "P"],
            "RNE_next": ["P", "DIFF", "P", "P", "P", "P", "P", None, None, "P", None],
            "LS": ["C2", "C2", "C2", "C2", "C2", "C2", "C2", None, "C2", "C2", "C2"],
            "LS_next": [
                "C2",
                "C2",
                "DIFF",
                "C2",
                "C2",
                "C2",
                "C2",
                None,
                None,
                "C2",
                None,
            ],
            "PLACE": ["U1", "U1", "U1", "U1", "U1", "U1", "U1", None, "U1", "U1", "U1"],
            "PLACE_next": [
                "U1",
                "U1",
                "U1",
                "DIFF",
                "U1",
                "U1",
                "U1",
                None,
                None,
                "U1",
                None,
            ],
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
                "PR1",
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
                "PR1",
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
                "ABC1",
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
                "ABC1",
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
                "SC1234",
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
                "SC1234",
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
        False,
        False,
    ]


def test__is_previous_episode_duplicate():
    data = pd.DataFrame(
        {
            "DEC": [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "2016-08-31",
                None,
            ],
            "Has_previous_episode": [
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                False,
            ],
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
                "2016-08-22",
                "2016-08-22",
            ],
            "DECOM_previous": [
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                "2016-01-22",
                None,
            ],
            "RNE": ["P", "P", "P", "P", "P", "P", "P", None, "P", "P", "P"],
            "RNE_previous": [
                "P",
                "DIFF",
                "P",
                "P",
                "P",
                "P",
                "P",
                None,
                None,
                "P",
                None,
            ],
            "LS": ["C2", "C2", "C2", "C2", "C2", "C2", "C2", None, "C2", "C2", "C2"],
            "LS_previous": [
                "C2",
                "C2",
                "DIFF",
                "C2",
                "C2",
                "C2",
                "C2",
                None,
                None,
                "C2",
                None,
            ],
            "PLACE": ["U1", "U1", "U1", "U1", "U1", "U1", "U1", None, "U1", "U1", "U1"],
            "PLACE_previous": [
                "U1",
                "U1",
                "U1",
                "DIFF",
                "U1",
                "U1",
                "U1",
                None,
                None,
                "U1",
                None,
            ],
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
                "PR1",
                "PR1",
            ],
            "PLACE_PROVIDER_previous": [
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "DIFF",
                "PR1",
                "PR1",
                None,
                None,
                "PR1",
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
                "ABC1",
                "ABC1",
            ],
            "PL_POST_previous": [
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "ABC1",
                "DIFF",
                "ABC1",
                None,
                None,
                "ABC1",
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
                "SC1234",
                "SC1234",
            ],
            "URN_previous": [
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "SC1234",
                "DIFF",
                None,
                None,
                "SC1234",
                None,
            ],
        }
    )

    data["Test result"] = _is_previous_episode_duplicate(data)
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
        False,
        False,
    ]


def test__is_previous_episode_submitted_later():
    data = pd.DataFrame(
        {
            "DEC": ["2016-01-22", None, None, None],
            "Has_previous_episode": [True, True, True, None],
            "YEAR": [2018, 2018, 2018, 2018],
            "YEAR_previous": [2018, 2019, 2017, None],
        }
    )
    data["Test result"] = _is_previous_episode_submitted_later(data)
    assert data["Test result"].tolist() == [
        False,
        True,
        False,
        False,
    ]


def test__stage1_rule_to_apply():
    data = pd.DataFrame(
        {
            "Has_open_episode_error": [False, True, True, True, True, True],
            "Next_episode_is_duplicate": [None, True, False, False, False, False],
            "Previous_episode_is_duplicate": [None, True, False, False, False, False],
            "Previous_episode_submitted_later": [
                None,
                False,
                True,
                False,
                False,
                False,
            ],
            "Has_next_episode": [None, False, False, False, True, True],
            "Has_next_episode_with_RNE_equals_S": [
                None,
                False,
                False,
                False,
                True,
                False,
            ],
        }
    )
    data["Rule_to_apply"] = data.apply(_stage1_rule_to_apply, axis=1)
    assert data["Rule_to_apply"].tolist() == [
        None,
        "RULE_3",
        "RULE_3A",
        "RULE_2",
        "RULE_1A",
        "RULE_1",
    ]


def test__update_dec_stage1():
    data = pd.DataFrame(
        {
            "DEC": [None, "2020-11-11", None, None, None, None],
            "Has_open_episode_error": [False, False, True, True, True, True],
            "Rule_to_apply": [None, None, "RULE_1", "RULE_1A", "RULE_1A", "RULE_2"],
            "YEAR": [2022, 2022, 2022, 2022, 2022, 2022],
            "DECOM_next": [
                "2021-05-05",
                "2021-08-29",
                "2022-01-01",
                "2022-02-02",
                "2022-05-20",
                None,
            ],
        }
    )
    data[["DEC", "DECOM_next"]] = data[["DEC", "DECOM_next"]].apply(
        pd.to_datetime, format="%Y-%m-%d"
    )
    data["DEC"] = data.apply(_update_dec_stage1, axis=1)
    assert data["DEC"].astype(str).tolist() == [
        "NaT",
        "2020-11-11",
        "2022-01-01",
        "2022-02-01",
        "2022-03-31",
        "2022-03-31",
    ]


def test__update_rec_stage1():
    data = pd.DataFrame(
        {
            "REC": [None, "E41", None, None, None],
            "Has_open_episode_error": [False, False, True, True, True],
            "Rule_to_apply": [None, None, "RULE_1", "RULE_1A", "RULE_2"],
        }
    )
    data["updated_REC"] = data.apply(_update_rec_stage1, axis=1)
    assert data["updated_REC"].tolist() == [
        None,
        "E41",
        "X1",
        "E99",
        "E99",
    ]


def test__update_reason_place_change_stage1():
    data = pd.DataFrame(
        {
            "REASON_PLACE_CHANGE": [
                "CAREPL",
                "CAREPL",
                "CAREPL",
                "CAREPL",
                "CAREPL",
                "CAREPL",
                "OTHER",
            ],
            "RNE_next": ["P", "P", "P", "B", "T", "U", "S"],
            "Has_open_episode_error": [False, False, True, True, True, True, True],
            "Rule_to_apply": [
                None,
                None,
                "RULE_1",
                "RULE_1",
                "RULE_1",
                "RULE_1",
                "RULE_2",
            ],
        }
    )
    data["REASON_PLACE_CHANGE"] = data.apply(_update_reason_place_change_stage1, axis=1)
    assert data["REASON_PLACE_CHANGE"].tolist() == [
        "CAREPL",
        "CAREPL",
        "LIIAF",
        "LIIAF",
        "LIIAF",
        "LIIAF",
        "OTHER",
    ]


def test__update_episode_source_stage1():
    data = pd.DataFrame(
        {
            "Episode_source": ["Original", "Original"],
            "Has_open_episode_error": [False, True],
            "Rule_to_apply": [None, "RULE_1"],
        }
    )
    data["Episode_source"] = data.apply(_update_episode_source_stage1, axis=1)
    assert data["Episode_source"].tolist() == [
        "Original",
        "RULE_1",
    ]


def test_overlaps_next_episode():
    data = pd.DataFrame(
        {
            "Has_next_episode": [False, True, True, True],
            "YEAR": [2020, 2020, 2020, 2020],
            "YEAR_next": [None, 2021, 2021, 2019],
            "DEC": ["2021-01-31", "2021-01-31", "2021-01-31", "2021-01-31"],
            "DECOM_next": [None, "2022-02-02", "2021-01-01", "2021-01-01"],
        }
    )
    data["test_result"] = data.apply(_overlaps_next_episode, axis=1)
    assert data["test_result"].tolist() == [
        False,
        False,
        True,
        False,
    ]


def test__has_x1_gap_before_next_episode():
    data = pd.DataFrame(
        {
            "Has_next_episode": [False, True, True, True],
            "YEAR": [2020, 2020, 2020, 2020],
            "YEAR_next": [None, 2021, 2021, 2019],
            "DEC": ["2021-01-31", "2021-01-31", "2021-01-31", "2021-01-31"],
            "DECOM_next": [None, "2021-01-31", "2021-03-01", "2021-03-01"],
            "REC": ["E43", "X1", "X1", "X1"],
        }
    )
    data["test_result"] = data.apply(_has_x1_gap_before_next_episode, axis=1)
    assert data["test_result"].tolist() == [
        False,
        False,
        True,
        False,
    ]


def test__stage2_rule_to_apply():
    data = pd.DataFrame(
        {
            "Overlaps_next_episode": [False, True, False],
            "Has_X1_gap_before_next_episode": [False, False, True],
        }
    )
    data["test_result"] = data.apply(_stage2_rule_to_apply, axis=1)
    assert data["test_result"].tolist() == [
        None,
        "RULE_4",
        "RULE_5",
    ]


def test__update_dec_stage2():
    data = pd.DataFrame(
        {
            "DEC": ["2021-01-01", "2021-01-01", "2021-01-01"],
            "DECOM_next": ["2022-11-11", "2022-11-11", "2022-11-11"],
            "Rule_to_apply": [None, "RULE_4", "RULE_5"],
        }
    )
    data["test_result"] = data.apply(_update_dec_stage2, axis=1)
    assert data["test_result"].tolist() == [
        "2021-01-01",
        "2022-11-11",
        "2022-11-11",
    ]


def test__update_episode_source_stage2():
    None


def test_add_stage2_rule_identifier_columns():
    None


def test_identify_stage2_rule_to_apply():
    None


def test_apply_stage2_rules():
    None


# python -m black "/workspaces/liia-tools/tests/s903/"
# poetry run coverage run -m pytest
