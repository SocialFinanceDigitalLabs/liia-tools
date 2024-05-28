from liiatools.datasets.s251.lds_s251_clean import cleaner

from sfdata_stream_parser import events


def test_clean_postcodes():
    event = events.Cell(cell="G62 7PS", config_dict={"string": "postcode"})
    cleaned_event = list(cleaner.clean_postcodes(event))[0]
    assert cleaned_event.cell == "G62 7PS"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="CW3 9PU", config_dict={"string": "postcode"})
    cleaned_event = list(cleaner.clean_postcodes(event))[0]
    assert cleaned_event.cell == "CW3 9PU"
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell="string", config_dict={"string": "postcode"})
    cleaned_event = list(cleaner.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(cell=123, config_dict={"string": "postcode"})
    cleaned_event = list(cleaner.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "1"

    event = events.Cell(cell="", config_dict={"string": "postcode"})
    cleaned_event = list(cleaner.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"

    event = events.Cell(cell=None, config_dict={"string": "postcode"})
    cleaned_event = list(cleaner.clean_postcodes(event))[0]
    assert cleaned_event.cell == ""
    assert cleaned_event.formatting_error == "0"
