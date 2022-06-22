import tempfile as tmp
from unittest.mock import patch
from pathlib import Path
from datetime import datetime

from liiatools.datasets.s903.lds_ssda903_clean import logger

from sfdata_stream_parser import events


def test_create_formatting_error_count():
    stream = (
        events.StartTable(table_name="AD1"),
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
            assert event.formatting_error_count == [
                "some_header",
                "some_header",
            ]

    stream = (
        events.StartTable(table_name="AD1"),
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
            assert event.formatting_error_count == [
                "some_header",
                "some_other_header",
            ]

    stream = (
        events.StartTable(table_name="AD1"),
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
            assert event.formatting_error_count == ["some_header"]


def test_blank_error_check():
    stream = logger.blank_error_check(
        [
            events.Cell(config_dict={"canbeblank": False}, cell="", error="0"),
            events.Cell(config_dict={"canbeblank": False}, cell=None, error="0"),
            events.Cell(config_dict={"canbeblank": False}, cell="", error="1"),
            events.Cell(config_dict={"canbeblank": False}, cell="string", error="0"),
            events.Cell(config_dict={"canbeblank": True}, cell="", error="0"),
        ]
    )
    stream = list(stream)
    assert stream[0].blank_error == "1"
    assert stream[1].blank_error == "1"
    assert "blank_error" not in stream[2].as_dict()
    assert "blank_error" not in stream[3].as_dict()
    assert "blank_error" not in stream[4].as_dict()


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
            assert event.blank_error_count == ["some_header"]

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
            assert event.blank_error_count == [
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
            assert event.extra_column_error == [
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
            assert event.extra_column_error is None

    stream = (
        events.StartTable(extra_columns=""),
        logger.ErrorTable(),
        events.EndTable(),
    )
    events_with_extra_column_error = list(logger.inherit_extra_column_error(stream))
    for event in events_with_extra_column_error:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.extra_column_error == ""

    stream = (events.StartTable(), logger.ErrorTable(), events.EndTable())
    events_with_extra_column_error = list(logger.inherit_extra_column_error(stream))
    for event in events_with_extra_column_error:
        if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
            assert event.extra_column_error == []


@patch("builtins.open", create=True)
def test_save_errors_la(mock_save):
    la_log_dir = tmp.gettempdir()
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

    stream = logger.save_errors_la(
        [
            logger.ErrorTable(
                filename="test_file",
                formatting_error_count=["CHILD", "CHILD", "AGE"],
                blank_error_count=["POSTCODE", "POSTCODE", "DATE"],
                table_name="List 1",
                extra_column_error=["list", "of", "headers"],
                              ),
        ],
        la_log_dir
    )
    stream = list(stream)

    mock_save.assert_called_once_with(f"{Path(la_log_dir, 'test_file')}_error_log_{start_time}.txt", "a")
    # mock_save.write.assert_called_once_with(f"test_file_{start_time}")

