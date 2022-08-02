import tablib
import tempfile as tmp

from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean import (
    file_creator,
)


def test_save_tables():
    data = tablib.Dataset(headers=["Name", "Age"])
    data.append(("Kenneth", 12))
    output = tmp.gettempdir()

    stream = file_creator.save_tables(
        [
            events.StartContainer(),
            events.StartTable(sheet_name="List 1"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 2"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 3"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 4"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 5"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 6"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 7"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 8"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 9"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 10"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 11"),
            file_creator.TableEvent(data=data),
            events.EndContainer(filename="test"),
        ],
        output,
    )
    stream = list(stream)
    if isinstance(stream, file_creator.TableEvent):
        assert stream.data is not None


def test_filter_rows():
    stream = file_creator.filter_rows(
        [
            file_creator.RowEvent(
                sheet_name="List 1", row={"Date of Contact": "some value"}
            ),
            file_creator.RowEvent(sheet_name="List 1", row={"Date of Contact": ""}),
            file_creator.RowEvent(sheet_name="List 1", row={"Date of Contact": None}),
            file_creator.RowEvent(
                sheet_name="List 2", row={"Date of Contact": "some value"}
            ),
            file_creator.RowEvent(sheet_name="List 2", row={"Date of Contact": ""}),
            file_creator.RowEvent(sheet_name="List 2", row={"Date of Contact": None}),
        ]
    )
    stream = list(stream)
    assert stream[0].filter == 0
    assert stream[1].filter == 1
    assert stream[2].filter == 1
    assert stream[3].filter == 0
    assert stream[4].filter == 0
    assert stream[5].filter == 0


def test_coalesce_row():
    stream = file_creator.coalesce_row(
        [
            events.StartRow(),
            events.Cell(column_header="Date of Contact", value="value_one"),
            events.Cell(column_header="Date of Contact", value="value_two"),
            events.Cell(column_header="Date of Contact", value="value_three"),
            events.Cell(column_header="Date of referral", value="some_date"),
            events.Cell(column_header="Unknown", value="anything"),
            events.EndRow(),
        ]
    )
    stream = list(stream)
    assert stream[1].row["Date of Contact"] == "value_one"
    assert stream[1].row["Date of Contact 2"] == "value_two"
    assert stream[1].row["Date of Contact 3"] == "value_three"
    assert stream[1].row["Date of referral"] == "some_date"
    assert "Unknown" not in stream[1].row


def test_create_tables():
    stream = file_creator.create_tables(
        [
            events.StartTable(matched_column_headers=["header_one", "header_two"]),
            file_creator.RowEvent(
                filter=0, row={"header_one": "value_one", "header_two": "value_two"}
            ),
            file_creator.RowEvent(
                filter=0, row={"header_one": "value_one", "header_two": "value_two"}
            ),
            file_creator.RowEvent(
                filter=1, row={"header_one": "value_one", "header_two": "value_two"}
            ),
            events.EndTable(),
        ],
        la_name="Barnet",
    )
    stream = list(stream)
    assert hasattr(stream[6], "data")

    data = stream[6].data.export("df")
    assert data.shape == (2, 3)
