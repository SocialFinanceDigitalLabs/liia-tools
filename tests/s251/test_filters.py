import pandas as pd

from liiatools.datasets.s251.lds_s251_clean import filters

from sfdata_stream_parser import events
from datetime import datetime


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
        cell=0.0,
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
        cell=pd.DataFrame(data={"value": [123, 456]}),
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
