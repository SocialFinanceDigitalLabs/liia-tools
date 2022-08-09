from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean import logger

list_1_columns = [
    "Child Unique ID",
    "Gender",
    "Ethnicity",
    "Date of Birth",
    "Age of Child (Years)",
    "Date of Contact",
    "Contact Source",
]


def test_create_error_table():
    stream = logger.create_error_table(
        [
            events.StartTable(sheet_name="List 1"),
            events.EndTable(),
        ]
    )
    stream = list(stream)
    assert isinstance(stream[1], logger.ErrorTable)
    assert stream[1].sheet_name == "List 1"


def test_create_error_list():
    stream = logger.create_error_list(
        [
            events.StartTable(),
            events.Cell(column_header="Child Unique ID", blank_error="1"),
            events.Cell(column_header="Child Unique ID", blank_error="1"),
            events.Cell(column_header="Gender", blank_error="1"),
            events.Cell(column_header="Gender"),
            logger.ErrorTable()
        ],
        error_name="blank_error"
    )
    stream = list(stream)
    assert stream[5].blank_error_list == ["Child Unique ID", "Child Unique ID", "Gender"]

    stream = logger.create_error_list(
        [
            events.StartTable(),
            events.Cell(column_header="Child Unique ID", formatting_error="1"),
            events.Cell(column_header="Child Unique ID", formatting_error="1"),
            events.Cell(column_header="Gender", formatting_error="1"),
            events.Cell(column_header="Gender"),
            logger.ErrorTable()
        ],
        error_name="formatting_error"
    )
    stream = list(stream)
    assert stream[5].formatting_error_list == ["Child Unique ID", "Child Unique ID", "Gender"]


def test_inherit_error():
    stream = logger.inherit_error(
        [
            events.StartTable(extra_columns=["extra_column", "extra_column_2"]),
            logger.ErrorTable(),
            events.EndTable(),
        ],
        error_name="extra_columns"
    )
    stream = list(stream)
    assert stream[1].extra_columns_error == ["extra_column", "extra_column_2"]

    stream = logger.inherit_error(
        [
            events.StartTable(duplicate_columns=["duplicate_column", "duplicate_column_2"]),
            logger.ErrorTable(),
            events.EndTable(),
        ],
        error_name="duplicate_columns"
    )
    stream = list(stream)
    assert stream[1].duplicate_columns_error == ["duplicate_column", "duplicate_column_2"]


def test_create_file_match_error():
    stream = logger.create_file_match_error(
        [
            events.StartTable(sheet_name="List 1"),
            events.StartTable(name="random_sheet", column_headers=["header_1", "header_2"]),
        ]
    )
    stream = list(stream)
    assert not hasattr(stream[0], "match_error")
    assert stream[1].match_error == "Failed to find a set of matching columns headers for sheet titled " \
                                    "'random_sheet' which contains column headers ['header_1', 'header_2']"


def test_missing_sheet_match():
    sheet_names = ["List 1", "List 2"]
    expected_sheet_names = ["List 1", "List 2"]
    assert logger._missing_sheet_match(sheet_names, expected_sheet_names) == []

    sheet_names = ["List 1", "unknown_sheet"]
    assert logger._missing_sheet_match(sheet_names, expected_sheet_names) == ["List 2"]


def test_create_missing_sheet_error():
    stream = logger.create_missing_sheet_error(
        [
            events.StartTable(sheet_name="List 1"),
            events.StartTable(sheet_name="List 2"),
            events.StartTable(sheet_name="List 3"),
            events.StartTable(sheet_name="List 4"),
            events.StartTable(sheet_name="List 5"),
            events.StartTable(sheet_name="List 6"),
            events.StartTable(sheet_name="List 7"),
            events.StartTable(sheet_name="List 8"),
            events.StartTable(sheet_name="List 9"),
            events.StartTable(sheet_name="List 10"),
            events.StartTable(sheet_name="List 11"),
            events.EndContainer(),
        ]
    )
    stream = list(stream)
    assert not hasattr(stream[11], "missing_sheet_error")

    stream = logger.create_missing_sheet_error(
        [
            events.StartTable(sheet_name="List 1"),
            events.StartTable(sheet_name="List 2"),
            events.StartTable(sheet_name="List 3"),
            events.StartTable(sheet_name="List 4"),
            events.StartTable(sheet_name="List 5"),
            events.StartTable(sheet_name="List 6"),
            events.StartTable(sheet_name="List 7"),
            events.StartTable(sheet_name="List 8"),
            events.StartTable(sheet_name="List 9"),
            events.StartTable(sheet_name="List 10"),
            events.EndContainer(),
        ]
    )
    stream = list(stream)
    assert stream[10].missing_sheet_error == "The following sheets are missing: 'List 11' so no output has been created"


def test_duplicate_columns():
    columns_list = list_1_columns + ["Child Unique ID"]
    assert logger._duplicate_columns(columns_list) == ["Child Unique ID"]


def test_duplicate_columns_error():
    stream = logger.duplicate_column_check(
        [
            events.StartTable(
                matched_column_headers=list_1_columns + ["Child Unique ID"],
                sheet_name="List 1",
            )
        ]
    )
    stream = list(stream)
    assert stream[0].duplicate_columns == f"Sheet with title List 1 contained the following duplicate " \
                                          f"column(s): 'Child Unique ID'"

    stream = logger.duplicate_column_check([events.StartTable(sheet_name="List 1")])
    stream = list(stream)
    assert stream[0] == events.StartTable(sheet_name="List 1")


def test_blank_error_check():
    stream = logger.blank_error_check(
        [
            events.Cell(
                other_config={"canbeblank": False},
                value="string",
                formatting_error="0",
            ),
            events.Cell(
                other_config={"canbeblank": False},
                value=0,
                formatting_error="0",
            ),
            events.Cell(
                other_config={"canbeblank": False},
                value="",
                formatting_error="0",
            ),
            events.Cell(
                other_config={"canbeblank": False}, value="", error="0", blank_row="1"
            ),
        ]
    )
    stream = list(stream)
    assert stream[0].value == "string"
    assert stream[1].value == 0
    assert stream[2].value == ""
    assert stream[2].blank_error == "1"
    assert not hasattr(stream[3], "blank_error")
