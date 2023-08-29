from datetime import datetime

from sfdata_stream_parser import events

from liiatools.ssda903_pipeline.lds_ssda903_clean import degrade


def test_degrade_postcodes():
    event = events.Cell(header="HOME_POST", cell="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == "E20 1"

    event = events.Cell(header="PL_POST", cell="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == "E20 1"

    event = events.Cell(header="PL_POST", cell=None)
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(header="PL_POST", cell="")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(header="", cell="value")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.cell == "value"


def test_degrade_dob():
    event = events.Cell(header="DOB", cell=datetime(2012, 4, 25).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == datetime(2012, 4, 1).date()

    event = events.Cell(header="MC_DOB", cell=datetime(2013, 7, 13).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == datetime(2013, 7, 1).date()

    event = events.Cell(header="MC_DOB", cell="")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(header="MC_DOB", cell=None)
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == ""

    event = events.Cell(header="", cell="value")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.cell == "value"
