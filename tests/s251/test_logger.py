import tempfile as tmp
from unittest.mock import patch
from pathlib import Path
from datetime import datetime

from liiatools.datasets.s251.lds_s251_clean import logger

from sfdata_stream_parser import events


def test_create_formatting_error_list():
    stream = (
        events.StartTable(),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_header", formatting_error="0"),
        events.EndTable(),
    )
    events_with_formatting_error_list = list(
        logger.create_formatting_error_list(stream)
    )
    for event in events_with_formatting_error_list:
        if isinstance(event, logger.ErrorTable):
            assert event.formatting_error_list == [
                "some_header",
                "some_header",
            ]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_other_header", formatting_error="1"),
        events.Cell(header="some_header"),
        events.EndTable(),
    )
    events_with_formatting_error_list = list(
        logger.create_formatting_error_list(stream)
    )
    for event in events_with_formatting_error_list:
        if isinstance(event, logger.ErrorTable):
            assert event.formatting_error_list == [
                "some_header",
                "some_other_header",
            ]

    stream = (
        events.StartTable(),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_header_2", formatting_error=None),
        events.Cell(header="some_header_3", formatting_error=""),
        events.Cell(),
        events.EndTable(),
    )
    events_with_formatting_error_list = list(
        logger.create_formatting_error_list(stream)
    )
    for event in events_with_formatting_error_list:
        if isinstance(event, logger.ErrorTable):
            assert event.formatting_error_list == ["some_header"]


def test_create_extra_column_error():
    stream = logger.create_extra_column_error(
        [
            events.StartTable(
                expected_columns=["column_1", "column_2"],
                headers=["column_1", "column_2"],
            )
        ]
    )
    event_without_extra_column_error = list(stream)
    assert "extra_column_error" not in event_without_extra_column_error[0].as_dict()

    stream = logger.create_extra_column_error(
        [
            events.StartTable(
                filename="test_file.csv",
                table_name="test_table",
                expected_columns=["column_1", "column_2"],
                headers=["column_1", "column_2", "column_3"],
            )
        ]
    )
    event_with_extra_column_error = list(stream)
    assert (
        event_with_extra_column_error[0].extra_column_error
        == "Additional columns were found in file titled "
        "'test_file.csv' than those expected from the "
        "schema so these columns have been removed: "
        "['column_3']"
    )


def test_create_file_match_error():
    config = {
        "placement_costs": {"Child ID": {"string": "alphanumeric", "canbeblank": False}}
    }

    stream = logger.create_file_match_error(
        [events.StartTable(expected_columns=["Child ID"])], config=config
    )
    event_without_file_match_error = list(stream)
    assert not hasattr(event_without_file_match_error[0], "match_error")

    stream = logger.create_file_match_error(
        [events.StartTable(filename="test_file.csv", headers=["column_1", "column_2"])],
        config=config,
    )
    event_with_file_match_error = list(stream)
    assert (
        event_with_file_match_error[0].match_error
        == "Failed to find a set of matching columns headers for file titled 'test_file.csv' which is "
        "missing column headers ['Child ID'] so no output has been produced"
    )


@patch("builtins.open", create=True)
def test_save_errors_la(mock_save):
    la_log_dir = tmp.gettempdir()
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"

    stream = logger.save_errors_la(
        [
            logger.ErrorTable(
                filename="test_file",
                formatting_error_list=["CHILD", "CHILD", "AGE"],
                blank_error_list=["POSTCODE", "POSTCODE", "DATE"],
                below_zero_error_list=["AGE"],
                extra_column_error=["list", "of", "headers"],
            ),
        ],
        la_log_dir,
    )
    stream = list(stream)

    mock_save.assert_called_once_with(
        f"{Path(la_log_dir, 'test_file')}_error_log_{start_time}.txt", "a"
    )
    # mock_save.write.assert_called_once_with(f"test_file_{start_time}")
