from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean.configuration import (
    identify_cell_header,
    add_sheet_name,
    Config,
    _match_column_name,
    match_property_config_to_cell,
    convert_column_header_to_match,
)

list_1_columns = [
    "Child Unique ID",
    "Gender",
    "Ethnicity",
    "Date of Birth",
    "Age of Child (Years)",
    "Date of Contact",
    "Contact Source",
]

cfg = Config()


def test_match_column_name():
    assert _match_column_name("Child Unique ID", "Child Unique ID") == "Child Unique ID"
    assert _match_column_name("child unique id", "Child Unique ID") == "Child Unique ID"
    assert (
        _match_column_name("   Child Unique ID   ", "Child Unique ID")
        == "Child Unique ID"
    )

    assert (
        _match_column_name("Child ID", "Child Unique ID", [r"/.*child.*id.*/i"])
        == "Child Unique ID"
    )
    assert (
        _match_column_name("child id", "Child Unique ID", [r"/.*child.*id.*/i"])
        == "Child Unique ID"
    )


def test_add_sheet_name_exact():
    stream = add_sheet_name(
        [events.StartTable(column_headers=list_1_columns)], config=cfg["datasources"]
    )
    stream = list(stream)
    assert stream[0].sheet_name == "List 1"
    assert stream[0].matched_column_headers == list_1_columns
    assert stream[0].extra_columns == []


def test_add_sheet_name_extra_header():
    stream = add_sheet_name(
        [
            events.StartTable(
                column_headers=list_1_columns + ["Extra Header 1", "Extra Header 2"]
            )
        ],
        config=cfg["datasources"],
    )
    stream = list(stream)
    assert stream[0].sheet_name == "List 1"
    assert stream[0].matched_column_headers == list_1_columns
    assert stream[0].extra_columns == ["Extra Header 1", "Extra Header 2"]


def test_add_sheet_name_missing():
    stream = add_sheet_name(
        [events.StartTable(column_headers=set(list_1_columns[1:]))],
        config=cfg["datasources"],
    )
    stream = list(stream)

    assert not hasattr(stream[0], "sheet_name")


def test_add_sheet_name_regex():
    my_cols = list(list_1_columns)
    my_cols[0] = "Child ID"
    stream = add_sheet_name(
        [events.StartTable(column_headers=my_cols)], config=cfg["datasources"]
    )
    stream = list(stream)

    assert stream[0].sheet_name == "List 1"
    assert stream[0].matched_column_headers == list_1_columns


def test_add_sheet_name_lower_case():
    my_cols = [c.lower() for c in list_1_columns]
    stream = add_sheet_name(
        [events.StartTable(column_headers=my_cols)], config=cfg["datasources"]
    )
    stream = list(stream)

    assert stream[0].sheet_name == "List 1"


def test_add_sheet_name_each_table():
    stream = add_sheet_name(
        [
            events.StartTable(column_headers=table_cfg.keys())
            for table_cfg in cfg["datasources"].values()
        ],
        config=cfg["datasources"],
    )
    stream = list(stream)

    table_names = [e.get("sheet_name") for e in stream]

    assert table_names == list(cfg["datasources"].keys())


def test_add_sheet_name_duplicate_match():
    stream = add_sheet_name(
        [events.StartTable(column_headers=list_1_columns + ["Child Unique ID"])],
        config=cfg["datasources"],
    )
    stream = list(stream)
    assert stream[0].sheet_name == "List 1"
    assert stream[0].matched_column_headers == list_1_columns + ["Child Unique ID"]


def test_identify_cell_header():
    stream = identify_cell_header(
        [
            events.Cell(column_index=0, column_headers=["a", "b", "c"]),
            events.Cell(column_index=1, column_headers=["a", "b", "c"]),
            events.Cell(column_index=2, column_headers=["a", "b", "c"]),
        ]
    )
    stream = list(stream)
    assert stream[0].column_header == "a"
    assert stream[1].column_header == "b"
    assert stream[2].column_header == "c"


def test_convert_column_header_to_match():
    stream = convert_column_header_to_match(
        [
            events.Cell(sheet_name="List 1", column_header="Child ID"),
            events.Cell(sheet_name="List 1", column_header="Date Birth"),
            events.Cell(sheet_name="List 1", column_header="random_column"),
            events.Cell(sheet_name="List 1", column_header="Gender"),
        ],
        config=cfg["datasources"],
    )
    stream = list(stream)
    assert stream[0].column_header == "Child Unique ID"
    assert stream[1].column_header == "Date of Birth"
    assert stream[2].column_header == "Unknown"
    assert stream[3].column_header == "Gender"


def test_match_property_config_to_cell():
    stream = match_property_config_to_cell(
        [
            events.Cell(column_header="Child Unique ID", sheet_name="List 1"),
            events.Cell(column_header="Gender", sheet_name="List 0"),
        ],
        config=cfg["datasources"],
        prop_name="other_config",
    )

    stream = list(stream)
    assert stream[0].other_config == {
        "regex": ["/.*child.*id.*/i"],
        "canbeblank": False,
    }
    assert stream[1] == events.Cell(column_header="Gender", sheet_name="List 0")

    stream = match_property_config_to_cell(
        [
            events.Cell(column_header="Gender", sheet_name="List 1"),
            events.Cell(column_header="Gender", sheet_name="List 0"),
        ],
        config=cfg["data_config"],
        prop_name="category_config",
    )

    stream = list(stream)
    assert stream[0].category_config == [
        {"code": "b) Female", "name": "F", "regex": ["/.*fem.*/i", "/b\\).*/i"]},
        {"code": "a) Male", "name": "M", "regex": ["/^mal.*/i", "/a\\).*/i"]},
        {
            "code": "c) Not stated/recorded",
            "name": "Not stated/recorded",
            "regex": [
                "/not.*/i",
                "/.*unknown.*/i",
                "/.*indeterminate.*/i",
                "/c\\).*/i",
            ],
        },
        {"code": "d) Neither", "name": "Neither", "regex": ["/d\\).*/i"]},
    ]
    assert stream[1] == events.Cell(column_header="Gender", sheet_name="List 0")
