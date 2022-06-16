from liiatools.datasets.s903.lds_ssda903_clean import configuration as config

from sfdata_stream_parser import events


def test_add_table_name():
    event = events.StartTable(
        headers=["CHILD", "SEX", "DOB", "ETHNIC", "UPN", "MOTHER", "MC_DOB"],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "Header"

    event = events.StartTable(
        headers=[
            "CHILD",
            "DECOM",
            "RNE",
            "LS",
            "CIN",
            "PLACE",
            "PLACE_PROVIDER",
            "DEC",
            "REC",
            "REASON_PLACE_CHANGE",
            "HOME_POST",
            "PL_POST",
            "URN",
        ],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "Episodes"

    event = events.StartTable(
        headers=["CHILD", "DOB", "REVIEW", "REVIEW_CODE"], filename="SSDA903_AD1.csv"
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "Reviews"

    event = events.StartTable(
        headers=["CHILD", "SEX", "DOB", "DUC"], filename="SSDA903_AD1.csv"
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "UASC"

    event = events.StartTable(
        headers=[
            "CHILD",
            "DOB",
            "SDQ_SCORE",
            "SDQ_REASON",
            "CONVICTED",
            "HEALTH_CHECK",
            "IMMUNISATIONS",
            "TEETH_CHECK",
            "HEALTH_ASSESSMENT",
            "SUBSTANCE_MISUSE",
            "INTERVENTION_RECEIVED",
            "INTERVENTION_OFFERED",
        ],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "OC2"

    event = events.StartTable(
        headers=["CHILD", "DOB", "IN_TOUCH", "ACTIV", "ACCOM"],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "OC3"

    event = events.StartTable(
        headers=[
            "CHILD",
            "DOB",
            "DATE_INT",
            "DATE_MATCH",
            "FOSTER_CARE",
            "NB_ADOPTR",
            "SEX_ADOPTR",
            "LS_ADOPTR",
        ],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "AD1"

    event = events.StartTable(
        headers=[
            "CHILD",
            "DOB",
            "DATE_PLACED",
            "DATE_PLACED_CEASED",
            "REASON_PLACED_CEASED",
        ],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "PlacedAdoption"

    event = events.StartTable(
        headers=["CHILD", "DOB", "PREV_PERM", "LA_PERM", "DATE_PERM"],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "PrevPerm"

    event = events.StartTable(
        headers=["CHILD", "DOB", "MISSING", "MIS_START", "MIS_END"],
        filename="SSDA903_AD1.csv",
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.table_name == "Missing"

    event = events.StartTable(
        headers=["incorrect", "header", "values"], filename="SSDA903_AD1.csv"
    )
    event_with_table_name = list(config.add_table_name(event))[0]
    assert (
        event_with_table_name.match_error
        == f"Failed to find a set of matching columns headers for "
        f"file '{event.filename}' which contains column headers "
        f"{event.headers}"
    )

    event = events.StartTable(headers=[""], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert (
        event_with_table_name.match_error
        == f"Failed to find a set of matching columns headers for "
        f"file '{event.filename}' which contains column headers "
        f"{event.headers}"
    )

    event = events.StartTable(headers=[None], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert (
        event_with_table_name.match_error
        == f"Failed to find a set of matching columns headers for "
        f"file '{event.filename}' which contains column headers "
        f"{event.headers}"
    )


def test_inherit_table_name():
    stream = (
        events.StartTable(table_name="AD1"),
        events.StartRow(),
        events.Cell(),
        events.EndRow(),
        events.EndTable(),
    )
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        assert event.table_name == "AD1"

    stream = (
        events.StartTable(table_name=""),
        events.StartRow(),
        events.Cell(),
        events.EndRow(),
        events.EndTable(),
    )
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        assert event.table_name == ""

    stream = (
        events.StartTable(table_name=None),
        events.StartRow(),
        events.Cell(),
        events.EndRow(),
        events.EndTable(),
    )
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        if isinstance(event, events.StartTable) or isinstance(event, events.EndTable):
            assert event.table_name is None
        else:
            assert event.as_dict() == {}

    stream = (
        events.StartTable(),
        events.StartRow(),
        events.Cell(),
        events.EndRow(),
        events.EndTable(),
    )
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        if isinstance(event, events.EndTable):
            assert event.table_name is None
        else:
            assert event.as_dict() == {}


def test_match_config_to_cell():
    event = events.Cell(table_name="AD1", header="DOB", filename="SSDA903_AD1.csv")
    config_dict = {"AD1": {"DOB": {"date": "%d/%m/%Y", "canbeblank": False}}}
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config.config_dict == config_dict["AD1"]["DOB"]

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = {
        "Episodes": {
            "RNE": {"category": [{"code": "S"}, {"code": "L"}], "canbeblank": False}
        }
    }
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config.config_dict == config_dict["Episodes"]["RNE"]

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = {}
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config == event

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = None
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config == event

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = 700
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config == event

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = "random_string"
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config == event
