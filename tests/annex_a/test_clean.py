from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean.cleaner import clean_integers


def test_clean_integers():
    stream = clean_integers(
        [
            events.Cell(column_index=0, cagegory_config={'canbeblank': False}, other_config={"type": "integer"}, value="5"),
            events.Cell(column_index=1, cagegory_config={'canbeblank': False}, other_config={"type": "integer"}, value=""),
            events.Cell(column_index=2, cagegory_config={'canbeblank': False}, other_config={"type": "integer"}, value="a"),
            events.Cell(column_index=3, cagegory_config={'canbeblank': False}, other_config={}, value="3"),
            events.Cell(column_index=4, cagegory_config={'canbeblank': False}, other_config={"type": "integer"}),
        ]
    )
    stream = list(stream)
    assert stream[0].value == 5
    assert stream[0].error == "0"
    assert stream[1].value == ''
    assert stream[1].error == "0"
    assert stream[2].value == ''
    assert stream[2].error == "1"
    assert stream[3].value == "3"
    assert stream[4].value == ""
    assert stream[4].error == "1"