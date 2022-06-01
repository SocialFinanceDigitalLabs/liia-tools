from liiatools.datasets.s903.lds_ssda903_clean import (config, converters, degrade, file_creator, filters, logger, parse, populate)

from liiatools.datasets.shared_functions import common

from sfdata_stream_parser import events
from datetime import datetime


def test_add_table_name():
    event = events.StartTable(headers=["CHILD", "SEX", "DOB", "ETHNIC", "UPN", "MOTHER", "MC_DOB"],
                              filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "Header"

    event = events.StartTable(headers=["CHILD", "DECOM", "RNE", "LS", "CIN", "PLACE", "PLACE_PROVIDER", "DEC", "REC",
                                       "REASON_PLACE_CHANGE", "HOME_POST", "PL_POST", "URN"],
                              filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "Episodes"

    event = events.StartTable(headers=["CHILD", "DOB", "REVIEW", "REVIEW_CODE"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "Reviews"

    event = events.StartTable(headers=["CHILD", "SEX", "DOB", "DUC"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "UASC"

    event = events.StartTable(headers=["CHILD", "DOB", "SDQ_SCORE", "SDQ_REASON", "CONVICTED", "HEALTH_CHECK",
                                       "IMMUNISATIONS", "TEETH_CHECK", "HEALTH_ASSESSMENT", "SUBSTANCE_MISUSE",
                                       "INTERVENTION_RECEIVED", "INTERVENTION_OFFERED"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "OC2"

    event = events.StartTable(headers=["CHILD", "DOB", "IN_TOUCH", "ACTIV", "ACCOM"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "OC3"

    event = events.StartTable(headers=["CHILD", "DOB", "DATE_INT", "DATE_MATCH", "FOSTER_CARE", "NB_ADOPTR",
                                       "SEX_ADOPTR", "LS_ADOPTR"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "AD1"

    event = events.StartTable(headers=["CHILD", "DOB", "DATE_PLACED", "DATE_PLACED_CEASED", "REASON_PLACED_CEASED"],
                              filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "PlacedAdoption"

    event = events.StartTable(headers=["CHILD", "DOB", "PREV_PERM", "LA_PERM", "DATE_PERM"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "PrevPerm"

    event = events.StartTable(headers=["CHILD", "DOB", "MISSING", "MIS_START", "MIS_END"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["table_name"] == "Missing"

    event = events.StartTable(headers=["incorrect", "header", "values"], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["match_error"] == f"Failed to find a set of matching columns headers for " \
                                                             f"file '{event.filename}' which contains column headers " \
                                                             f"{event.headers}"

    event = events.StartTable(headers=[""], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["match_error"] == f"Failed to find a set of matching columns headers for " \
                                                             f"file '{event.filename}' which contains column headers " \
                                                             f"{event.headers}"

    event = events.StartTable(headers=[None], filename="SSDA903_AD1.csv")
    event_with_table_name = list(config.add_table_name(event))[0]
    assert event_with_table_name.as_dict()["match_error"] == f"Failed to find a set of matching columns headers for " \
                                                             f"file '{event.filename}' which contains column headers " \
                                                             f"{event.headers}"


def test_inherit_table_name():
    stream = events.StartTable(table_name="AD1"), events.StartRow(), events.Cell(), events.EndRow(), events.EndTable()
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        assert event.as_dict()["table_name"] == "AD1"

    stream = events.StartTable(table_name=""), events.StartRow(), events.Cell(), events.EndRow(), events.EndTable()
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        assert event.as_dict()["table_name"] == ""

    stream = events.StartTable(table_name=None), events.StartRow(), events.Cell(), events.EndRow(), events.EndTable()
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        if isinstance(event, events.StartTable) or isinstance(event, events.EndTable):
            assert event.as_dict()["table_name"] is None
        else:
            assert event.as_dict() == {}

    stream = events.StartTable(), events.StartRow(), events.Cell(), events.EndRow(), events.EndTable()
    events_with_table_name = list(config.inherit_table_name(stream))
    for event in events_with_table_name:
        if isinstance(event, events.EndTable):
            assert event.as_dict()["table_name"] is None
        else:
            assert event.as_dict() == {}


def test_match_config_to_cell():
    event = events.Cell(table_name="AD1", header="DOB", filename="SSDA903_AD1.csv")
    config_dict = {"AD1": {"DOB": {"date": "%d/%m/%Y", "canbeblank": False}}}
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config.as_dict()["config_dict"] == config_dict["AD1"]["DOB"]

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = {"Episodes": {"RNE": {"category": [{"code": "S"}, {"code": "L"}], "canbeblank": False}}}
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config.as_dict()["config_dict"] == config_dict["Episodes"]["RNE"]

    event = events.Cell(table_name="Episodes", header="RNE", filename="SSDA903_AD1.csv")
    config_dict = {}
    event_with_config = list(config.match_config_to_cell(event, config=config_dict))[0]
    assert event_with_config.as_dict() == event.as_dict()

test_match_config_to_cell()


def test_to_category():
    category_dict = [{"code": "M1"}, {"code": "F1"}, {"code": "MM"}, {"code": "FF"}, {"code": "MF"}]
    assert converters.to_category("M1", category_dict) == "M1"
    assert converters.to_category("M2", category_dict) is None
    assert converters.to_category("", category_dict) is None


def test_to_integer():
    assert converters.to_integer("3000", "integer") == 3000
    assert converters.to_integer(123, "integer") == 123
    assert converters.to_integer("date", "") == "date"
    assert converters.to_integer("", "integer") == ""
    assert converters.to_integer(None, "integer") == ""


def test_to_date():
    assert converters.to_date("15/01/2001", "%d/%m/%Y") == datetime(2001, 1, 15).date()
    assert converters.to_date(datetime(2001, 1, 15), "%d/%m/%Y") == datetime(2001, 1, 15).date()
    assert converters.to_date(None, "%d/%m/%Y") == ""
    assert converters.to_date("", "%d/%m/%Y") == ""


def test_check_postcode():
    assert common.check_postcode("ME17 4JX") == "ME17 4JX"
    assert common.check_postcode("NW1 3LP") == "NW1 3LP"
    assert common.check_postcode("n2 7fe") == "n2 7fe"
    assert common.check_postcode("l38pj") == "l38pj"
    assert common.check_postcode("ME17 4JX17") == "ME17 4JX"
    assert common.check_postcode("X7ME17 4JX17") == "ME17 4JX"
    assert common.check_postcode("") == ""
    assert common.check_postcode(None) == ""


def test_to_short_postcode():
    assert common.to_short_postcode("ME17 4JX") == "ME17 4"
    assert common.to_short_postcode("NW1 3LP") == "NW1 3"
    assert common.to_short_postcode("n2 7fe") == "n2 7"
    assert common.to_short_postcode("l38pj") == "l38"
    assert common.to_short_postcode(12) == ""
    assert common.to_short_postcode(datetime(2004, 3, 18)) == ""


def test_to_month_only_dob():
    assert common.to_month_only_dob(datetime(2004, 3, 18).date()) == datetime(2004, 3, 1).date()
    assert common.to_month_only_dob(2008) == ""
    assert common.to_month_only_dob("12/1/1999") == ""


def test_degrade_postcodes():
    event = events.Cell(header="HOME_POST", cell="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.as_dict()["cell"] == "E20 1"

    event = events.Cell(header="PL_POST", cell="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.as_dict()["cell"] == "E20 1"

    event = events.Cell(header="PL_POST", cell=None)
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.as_dict()["cell"] == ""

    event = events.Cell(header="PL_POST", cell="")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.as_dict()["cell"] == ""

    event = events.Cell(header="", cell="value")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.as_dict()["cell"] == "value"


def test_degrade_dob():
    event = events.Cell(header="DOB", cell=datetime(2012, 4, 25).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.as_dict()["cell"] == datetime(2012, 4, 1).date()

    event = events.Cell(header="MC_DOB", cell=datetime(2013, 7, 13).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.as_dict()["cell"] == datetime(2013, 7, 1).date()

    event = events.Cell(header="MC_DOB", cell="")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.as_dict()["cell"] == ""

    event = events.Cell(header="MC_DOB", cell=None)
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.as_dict()["cell"] == ""

    event = events.Cell(header="", cell="value")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.as_dict()["cell"] == "value"


# file_creator.coalesce_row()
# file_creator.create_tables()
# file_creator.save_tables()

# filters.clean_dates()
# filters.clean_categories()
# filters.clean_integers()
# filters.clean_postcodes()

# logger.create_formatting_error_count()
# logger.blank_error_check()
# logger.create_blank_error_count()
# logger.inherit_extra_column_error()
# logger.save_errors_la()

# parse.findfiles()
# parse.add_filename()
# parse.parse_csv()

# populate.add_year_column()
# populate.create_la_child_id()
