import tablib
import tempfile as tmp
from unittest.mock import patch, mock_open
import os

from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean import file_creator


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
    if isinstance(stream, events.EndContainer):
        open_mock = mock_open()
        with patch("builtins.open", open_mock, create=True):
            file_creator.save_tables("test-data", output)
        open_mock.assert_called_with(
            f"{os.path.join(output, stream.filename)}_clean.xlsx", "wb"
        )
