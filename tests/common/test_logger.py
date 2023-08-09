from liiatools.datasets.shared_functions import logger

from sfdata_stream_parser import events


def test_blank_error_check():
    stream = logger.blank_error_check(
        [
            events.Cell(
                config_dict={"canbeblank": False},
                cell="",
                formatting_error="0",
                below_zero_error="0",
            ),
            events.Cell(
                config_dict={"canbeblank": False},
                cell=None,
                formatting_error="0",
                below_zero_error="0",
            ),
            events.Cell(
                config_dict={"canbeblank": False},
                cell="",
                formatting_error="1",
                below_zero_error="0",
            ),
            events.Cell(
                config_dict={"canbeblank": False},
                cell="string",
                formatting_error="0",
                below_zero_error="0",
            ),
            events.Cell(
                config_dict={"canbeblank": True},
                cell="",
                formatting_error="0",
                below_zero_error="0",
            ),
            events.Cell(
                config_dict={"canbeblank": False},
                cell="",
                formatting_error="0",
                below_zero_error="1",
            ),
        ]
    )
    stream = list(stream)
    assert stream[0].blank_error == "1"
    assert stream[1].blank_error == "1"
    assert not hasattr(stream[2], "blank_error")
    assert not hasattr(stream[3], "blank_error")
    assert not hasattr(stream[4], "blank_error")
    assert not hasattr(stream[5], "blank_error")


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


def test_create_below_zero_error_list():
    stream = (
        events.StartTable(),
        events.Cell(header="some_header", below_zero_error="1"),
        events.Cell(header="some_header_2", below_zero_error=None),
        events.Cell(header="some_header_3", below_zero_error=""),
        events.Cell(),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_below_zero_error_list = list(
        logger.create_below_zero_error_list(stream)
    )
    for event in events_with_below_zero_error_list:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.below_zero_error_list == ["some_header"]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", below_zero_error="1"),
        events.Cell(header="some_header_2", below_zero_error="1"),
        events.Cell(header="some_header_3", below_zero_error=""),
        events.Cell(),
        logger.ErrorTable(),
    )
    events_with_below_zero_error_list = list(
        logger.create_below_zero_error_list(stream)
    )
    for event in events_with_below_zero_error_list:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.below_zero_error_list == [
                "some_header",
                "some_header_2",
            ]
