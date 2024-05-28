from liiatools.datasets.shared_functions import cleaner

from sfdata_stream_parser import events
from datetime import datetime


def test_clean_dates():
    event = events.Cell(cell=datetime(2019, 1, 15), config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15).date()
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="2019/1/15", config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(
        cell=datetime(2019, 1, 15), config_dict={"not_date": "%d/%m/%Y"}
    )
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15)

    event = events.Cell(cell="string", config_dict={"not_date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.cell == "string"

    event = events.Cell(cell=None, config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="", config_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"


def test_clean_categories():
    event = events.Cell(
        cell="0",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == "0"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(
        cell="0.0",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == "0"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(
        cell=0,
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == "0"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(
        cell="true",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == "1"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(
        cell=123,
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(
        cell="string",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(
        cell="string",
        config_dict={
            "not_category": [
                {"code": "0", "name": "False"},
                {"code": "1", "name": "True"},
            ]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == "string"

    event = events.Cell(
        cell=None,
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(
        cell="",
        config_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"


def test_clean_integers():
    event = events.Cell(cell=123, config_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == 123
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="", config_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell=None, config_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="123", config_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == 123
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="string", config_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(cell=datetime(2017, 3, 17), config_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(
        cell=datetime(2017, 3, 17), config_dict={"not_numeric": "integer"}
    )
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == datetime(2017, 3, 17)

    event = events.Cell(cell="-12", config_dict={"numeric": "currency"})
    cleaned_event = list(cleaner.clean_integers(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.below_zero_error == "1"
