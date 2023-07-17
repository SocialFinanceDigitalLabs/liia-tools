import tempfile as tmp
import xmlschema
from unittest.mock import patch
from pathlib import Path
from datetime import datetime

from liiatools.datasets.social_work_workforce.lds_csww_clean import logger

from sfdata_stream_parser import events


def test_create_formatting_error_list():
    stream = (
        events.StartTable(table_name="AD1"),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_header", formatting_error="0"),
        events.EndTable(),
    )
    events_with_formatting_error_count = list(
        logger.create_formatting_error_list(stream)
    )
    for event in events_with_formatting_error_count:
        if isinstance(event, logger.ErrorTable):
            assert event.formatting_error_count == [
                "some_header",
                "some_header",
            ]

    stream = (
        events.StartTable(table_name="AD1"),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_other_header", formatting_error="1"),
        events.Cell(header="some_header"),
        events.EndTable(),
    )
    events_with_formatting_error_count = list(
        logger.create_formatting_error_list(stream)
    )
    for event in events_with_formatting_error_count:
        if isinstance(event, logger.ErrorTable):
            assert event.formatting_error_count == [
                "some_header",
                "some_other_header",
            ]

    stream = (
        events.StartTable(table_name="AD1"),
        events.Cell(header="some_header", formatting_error="1"),
        events.Cell(header="some_header_2", formatting_error=None),
        events.Cell(header="some_header_3", formatting_error=""),
        events.Cell(),
        events.EndTable(),
    )
    events_with_formatting_error_count = list(
        logger.create_formatting_error_list(stream)
    )
    for event in events_with_formatting_error_count:
        if isinstance(event, logger.ErrorTable):
            assert event.formatting_error_count == ["some_header"]


def test_blank_error_check():
    stream = logger.blank_error_check(
        [
            events.TextNode(schema_dict={"canbeblank": False}, text="", formatting_error="0"),
            events.TextNode(schema_dict={"canbeblank": False}, text=None, formatting_error="0"),
            events.TextNode(schema_dict={"canbeblank": False}, text="", formatting_error="1"),
            events.TextNode(schema_dict={"canbeblank": False}, text="string", formatting_error="0"),
            events.TextNode(schema_dict={"canbeblank": True}, text="", formatting_error="0"),
        ]
    )
    stream = list(stream)
    assert stream[0].blank_error == "1"
    assert stream[1].blank_error == "1"
    assert "blank_error" not in stream[2].as_dict()
    assert "blank_error" not in stream[3].as_dict()
    assert "blank_error" not in stream[4].as_dict()


# def test_create_blank_error_list():
#     schema = xmlschema.XsdElement('some_header',"xxx",None,True)
#     stream = (
#         events.StartElement(tag="LALevelVacancies"),
#         events.TextNode(text="text_1", schema=schema, blank_error="1"),
#         # events.TextNode(text="text_2", schema={'name': 'some_header_2'}, blank_error=None),
#         # events.TextNode(text="text_3", schema={'name': 'some_header_3'}, blank_error=""),
#         # events.TextNode(text="text_4", schema="header {'name': 'some_header_4'}')"),
#         events.EndElement(tag="Message"),
#         logger.ErrorTable(),
#     )
#     events_with_blank_error_count = list(logger.create_blank_error_list(stream))
#     print(f"blank error headers = {events_with_blank_error_count}")
#     for event in events_with_blank_error_count:
#         print(event.schema.name)
#         if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
#             assert event.blank_error_count == ["some_header"]

#     stream = (
#         events.StartElement(tag="LALevelVacancies"),
#         events.TextNode(text="some_header", blank_error="1"),
#         events.TextNode(text="some_header_2", blank_error="1"),
#         events.TextNode(text="some_header_3", blank_error=""),
#         events.TextNode(text="some_header_4"),
#         events.EndElement(tag="Message"),
#         logger.ErrorTable(),
#     )
#     events_with_blank_error_list = list(logger.create_blank_error_list(stream))
#     for event in events_with_blank_error_list:
#         if isinstance(event, logger.ErrorTable) and event.as_dict() != {}:
#             print(event.blank_error_count)
#             assert event.blank_error_count == [
#                 "some_header",
#                 "some_header_2",
#             ]


@patch("builtins.open", create=True)
def test_save_errors_la(mock_save):
    la_log_dir = tmp.gettempdir()
    start_time = f"{datetime.now():%Y-%m-%dT%H%M%SZ}"

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
        la_log_dir,
    )
    stream = list(stream)

    mock_save.assert_called_once_with(
        f"{Path(la_log_dir, 'test_file')}_error_log_{start_time}.txt", "a"
    )
    # mock_save.write.assert_called_once_with(f"test_file_{start_time}")

test_create_formatting_error_list()
test_blank_error_check()
test_create_blank_error_list()
# test_save_errors_la()


