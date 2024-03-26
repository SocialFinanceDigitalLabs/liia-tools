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
    None


def test__stage1_rule_to_apply():
    None


def test_add_stage1_rule_identifier_columns():
    None


def test_identify_stage1_rule_to_apply():
    None


def test__update_dec_stage1():
    None


def test__update_rec_stage1():
    None


def test__update_reason_place_change_stage1():
    None


def test__update_episode_source_stage1():
    None


def test_apply_stage1_rules():
    None


def test_overlaps_next_episode():
    None


def test__has_x1_gap_before_next_episode():
    None


def test__stage2_rule_to_apply():
    None


def test__update_dec_stage2():
    None


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
