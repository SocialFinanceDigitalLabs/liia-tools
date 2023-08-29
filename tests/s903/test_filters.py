from datetime import datetime

from sfdata_stream_parser import events

from liiatools.ssda903_pipeline.lds_ssda903_clean import filters


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
    event_with_table_name = list(config.add_table_name(event))
    table_name = getattr(event_with_table_name, "table_name", [])
    assert table_name == []

    event = events.StartTable(headers=[""], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))
    table_name = getattr(event_with_table_name, "table_name", [])
    assert table_name == []

    event = events.StartTable(headers=[None], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))
    table_name = getattr(event_with_table_name, "table_name", [])
    assert table_name == []


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


def test_clean_dates():
    event = events.Cell(cell=datetime(2019, 1, 15), config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(filters.clean_dates(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15).date()
    assert cleaned_event.error == "0"

    event = events.Cell(cell="2019/1/15", config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(filters.clean_dates(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(
        cell=datetime(2019, 1, 15), config_dict={"not_date": "%d/%m/%Y"}
    )
    cleaned_event = list(filters.clean_dates(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15)

    event = events.Cell(cell="string", config_dict={"not_date": "%d/%m/%Y"})
    cleaned_event = list(filters.clean_dates(event))[0]
    assert cleaned_event.cell == "string"

    event = events.Cell(cell=None, config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(filters.clean_dates(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"

    event = events.Cell(cell="", config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(filters.clean_dates(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"


def test_clean_categories():
    event = events.Cell(
        cell="0",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == "0"
    assert cleaned_event.error == "0"

    event = events.Cell(
        cell="0.0",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == "0"
    assert cleaned_event.error == "0"

    event = events.Cell(
        cell=0,
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == "0"
    assert cleaned_event.error == "0"

    event = events.Cell(
        cell="true",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == "1"
    assert cleaned_event.error == "0"

    event = events.Cell(
        cell=123,
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(
        cell="string",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(
        cell="string",
        config_dict={
            "not_category": [
                {"code": "0", "name": "False"},
                {"code": "1", "name": "True"},
            ]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == "string"

    event = events.Cell(
        cell=None,
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"

    event = events.Cell(
        cell="",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(filters.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"


def test_clean_integers():
    event = events.Cell(cell=123, config_dict={"numeric": "integer"})
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == 123
    assert cleaned_event.error == "0"

    event = events.Cell(cell="", config_dict={"numeric": "integer"})
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"

    event = events.Cell(cell=None, config_dict={"numeric": "integer"})
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"

    event = events.Cell(cell="123", config_dict={"numeric": "integer"})
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == 123
    assert cleaned_event.error == "0"

    event = events.Cell(cell="string", config_dict={"numeric": "integer"})
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(cell=datetime(2017, 3, 17), config_dict={"numeric": "integer"})
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(
        cell=datetime(2017, 3, 17), config_dict={"not_numeric": "integer"}
    )
    cleaned_event = list(filters.clean_integers(event))[0]
    assert cleaned_event.cell == datetime(2017, 3, 17)


def test_clean_postcodes():
    event = events.Cell(header="HOME_POST", cell="G62 7PS")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == "G62 7PS"
    assert cleaned_event.error == "0"

    event = events.Cell(header="PL_POST", cell="CW3 9PU")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == "CW3 9PU"
    assert cleaned_event.error == "0"

    event = events.Cell(header="PL_POST", cell="string")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(header="PL_POST", cell=123)
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "1"

    event = events.Cell(header="PL_POST", cell="")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"

    event = events.Cell(header="PL_POST", cell=None)
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.error == "0"
