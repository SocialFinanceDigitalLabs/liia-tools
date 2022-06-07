from liiatools.datasets.s903.lds_ssda903_clean import logger

from sfdata_stream_parser import events
from datetime import datetime
import tablib


def test_create_formatting_error_count():
    stream = (
        events.StartTable(),
        events.Cell(header="some_header", error="1"),
        events.Cell(header="some_header", error="1"),
        events.Cell(header="some_header", error="0"),
        events.EndTable(),
    )
    events_with_formatting_error_count = list(
        logger.create_formatting_error_count(stream)
    )
    for event in events_with_formatting_error_count:
        if isinstance(event, logger.ErrorTable):
            assert event.as_dict()["formatting_error_count"] == [
                "some_header",
                "some_header",
            ]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", error="1"),
        events.Cell(header="some_other_header", error="1"),
        events.Cell(header="some_header"),
        events.EndTable(),
    )
    events_with_formatting_error_count = list(
        logger.create_formatting_error_count(stream)
    )
    for event in events_with_formatting_error_count:
        if isinstance(event, logger.ErrorTable):
            assert event.as_dict()["formatting_error_count"] == [
                "some_header",
                "some_other_header",
            ]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", error="1"),
        events.Cell(header="some_header_2", error=None),
        events.Cell(header="some_header_3", error=""),
        events.Cell(),
        events.EndTable(),
    )
    events_with_formatting_error_count = list(
        logger.create_formatting_error_count(stream)
    )
    for event in events_with_formatting_error_count:
        if isinstance(event, logger.ErrorTable):
            assert event.as_dict()["formatting_error_count"] == ["some_header"]


def test_blank_error_check():
    event = events.Cell(cell=None, config_dict={"canbeblank": False})
    checked_event = list(logger.blank_error_check(event))[0]
    assert checked_event.as_dict()["cell"] is None
    assert checked_event.as_dict()["blank_error"] == "1"

    event = events.Cell(cell="", config_dict={"canbeblank": False})
    checked_event = list(logger.blank_error_check(event))[0]
    assert checked_event.as_dict()["cell"] == ""
    assert checked_event.as_dict()["blank_error"] == "1"

    event = events.Cell(cell="string", config_dict={"canbeblank": False})
    checked_event = list(logger.blank_error_check(event))[0]
    assert checked_event.as_dict()["cell"] == "string"

    event = events.Cell(cell="", config_dict={"canbeblank": True})
    checked_event = list(logger.blank_error_check(event))[0]
    assert checked_event.as_dict()["cell"] == ""

    event = events.Cell(cell=None, config_dict={"canbeblank": True})
    checked_event = list(logger.blank_error_check(event))[0]
    assert checked_event.as_dict()["cell"] is None

    event = events.Cell(cell="string", config_dict={"no_canbeblank": False})
    checked_event = list(logger.blank_error_check(event))[0]
    assert checked_event.as_dict()["cell"] == "string"


def test_create_blank_error_count():
    stream = (
        events.StartTable(),
        events.Cell(header="some_header", blank_error="1"),
        events.Cell(header="some_header_2", blank_error=None),
        events.Cell(header="some_header_3", blank_error=""),
        events.Cell(),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_blank_error_count = list(logger.create_blank_error_count(stream))
    for event in events_with_blank_error_count:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.as_dict()["blank_error_count"] == ["some_header"]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", blank_error="1"),
        events.Cell(header="some_header_2", blank_error="1"),
        events.Cell(header="some_header_3", blank_error=""),
        events.Cell(),
        logger.ErrorTable(),
    )
    events_with_blank_error_count = list(logger.create_blank_error_count(stream))
    for event in events_with_blank_error_count:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.as_dict()["blank_error_count"] == [
                "some_header",
                "some_header_2",
            ]


def test_inherit_extra_column_error():
    stream = (
        events.StartTable(extra_columns=["list", "of", "extra", "columns"]),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_extra_column_error = list(logger.inherit_extra_column_error(stream))
    for event in events_with_extra_column_error:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.as_dict()["extra_column_error"] == [
                "list",
                "of",
                "extra",
                "columns",
            ]

    stream = (
        events.StartTable(extra_columns=None),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_extra_column_error = list(logger.inherit_extra_column_error(stream))
    for event in events_with_extra_column_error:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.as_dict()["extra_column_error"] is None

    stream = (
        events.StartTable(extra_columns=""),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_extra_column_error = list(logger.inherit_extra_column_error(stream))
    for event in events_with_extra_column_error:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.as_dict()["extra_column_error"] == ""

    stream = (events.StartTable(), logger.ErrorTable(), events.EndTable())
    events_with_extra_column_error = list(logger.inherit_extra_column_error(stream))
    for event in events_with_extra_column_error:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.as_dict()["extra_column_error"] == []


# def test_save_errors_la():
# logger.save_errors_la()
