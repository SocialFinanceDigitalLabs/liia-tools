from liiatools.datasets.shared_functions import logger

from sfdata_stream_parser import events


def test_blank_error_check():
    stream = logger.blank_error_check(
        [
            events.Cell(
                config_dict={"canbeblank": False}, cell="", formatting_error="0"
            ),
            events.Cell(
                config_dict={"canbeblank": False}, cell=None, formatting_error="0"
            ),
            events.Cell(
                config_dict={"canbeblank": False}, cell="", formatting_error="1"
            ),
            events.Cell(
                config_dict={"canbeblank": False}, cell="string", formatting_error="0"
            ),
            events.Cell(
                config_dict={"canbeblank": True}, cell="", formatting_error="0"
            ),
        ]
    )
    stream = list(stream)
    assert stream[0].blank_error == "1"
    assert stream[1].blank_error == "1"
    assert "blank_error" not in stream[2].as_dict()
    assert "blank_error" not in stream[3].as_dict()
    assert "blank_error" not in stream[4].as_dict()


def test_create_blank_error_list():
    stream = (
        events.StartTable(),
        events.Cell(header="some_header", blank_error="1"),
        events.Cell(header="some_header_2", blank_error=None),
        events.Cell(header="some_header_3", blank_error=""),
        events.Cell(),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_blank_error_list = list(logger.create_blank_error_list(stream))
    for event in events_with_blank_error_list:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.blank_error_list == ["some_header"]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", blank_error="1"),
        events.Cell(header="some_header_2", blank_error="1"),
        events.Cell(header="some_header_3", blank_error=""),
        events.Cell(),
        logger.ErrorTable(),
    )
    events_with_blank_error_list = list(logger.create_blank_error_list(stream))
    for event in events_with_blank_error_list:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.blank_error_list == [
                "some_header",
                "some_header_2",
            ]


def test_create_file_match_error():
    stream = logger.create_file_match_error(
        [events.StartTable(expected_columns=["column_1", "column_2"])]
    )
    event_without_file_match_error = list(stream)
    assert "match_error" not in event_without_file_match_error[0].as_dict()

    stream = logger.create_file_match_error(
        [events.StartTable(filename="test_file.csv", headers=["column_1", "column_2"])]
    )
    event_with_file_match_error = list(stream)
    assert (
        event_with_file_match_error[0].match_error
        == "Failed to find a set of matching columns headers for "
        "file titled 'test_file.csv' which contains column headers "
        "['column_1', 'column_2'] so no output has been produced"
    )
