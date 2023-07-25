from liiatools.datasets.s251.lds_s251_clean import degrade

from sfdata_stream_parser import events
from datetime import datetime


def test_degrade_postcodes():
    config_dict = {"string": "postcode"}
    event = events.Cell(config_dict=config_dict, cell="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == "E20 1"

    event = events.Cell(config_dict=config_dict, cell="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == "E20 1"

    event = events.Cell(config_dict=config_dict, cell=None)
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(config_dict=config_dict, cell="")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == ""

    config_dict = {"string": "not_postcode"}
    event = events.Cell(config_dict=config_dict, cell="value")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == "value"


def test_degrade_dob():
    event = events.Cell(header="Date of birth", cell=datetime(2012, 4, 25).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == datetime(2012, 4, 1).date()

    event = events.Cell(header="Date of birth", cell=datetime(2013, 7, 13).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == datetime(2013, 7, 1).date()

    event = events.Cell(header="Date of birth", cell="")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(header="Date of birth", cell=None)
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(header="", cell="value")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == "value"
