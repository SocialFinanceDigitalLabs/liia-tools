from liiatools.datasets.annex_a.lds_annexa_clean import degrade

from sfdata_stream_parser import events
from datetime import datetime


def test_degrade_postcodes():
    event = events.Cell(column_header="Placement postcode", value="E20 1LP")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.value == "E20 1"

    event = events.Cell(column_header="Placement postcode", value="  E20  1LP    ")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.value == "E20 1"

    event = events.Cell(column_header="Placement postcode", value=None)
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.value == ""

    event = events.Cell(column_header="Placement postcode", value="")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.value == ""

    event = events.Cell(column_header="", value="value")
    degraded_event = list(degrade.degrade_postcodes(event))[0]
    assert degraded_event.value == "value"


def test_degrade_dob():
    event = events.Cell(column_header="Date of Birth", value=datetime(2012, 4, 25).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.value == datetime(2012, 4, 1).date()

    event = events.Cell(column_header="Date of Birth", value=datetime(2013, 7, 13).date())
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.value == datetime(2013, 7, 1).date()

    event = events.Cell(column_header="Date of Birth", value="")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.value == ""

    event = events.Cell(column_header="Date of Birth", value=None)
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.value == ""

    event = events.Cell(column_header="", value="value")
    degraded_event = list(degrade.degrade_dob(event))[0]
    assert degraded_event.value == "value"
