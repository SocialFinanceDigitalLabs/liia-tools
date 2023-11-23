from liiatools.datasets.s903.lds_ssda903_clean import filters

from sfdata_stream_parser import events


def test_clean_postcodes():
    event = events.Cell(header="HOME_POST", cell="G62 7PS")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == "G62 7PS"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(header="PL_POST", cell="CW3 9PU")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == "CW3 9PU"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(header="PL_POST", cell="string")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(header="PL_POST", cell=123)
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(header="PL_POST", cell="")
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(header="PL_POST", cell=None)
    cleaned_event = list(filters.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"
