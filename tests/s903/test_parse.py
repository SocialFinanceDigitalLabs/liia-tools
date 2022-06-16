from pathlib import Path
import tempfile as tmp
from unittest.mock import patch, mock_open
import tablib

from liiatools.datasets.s903.lds_ssda903_clean import parse

from sfdata_stream_parser import events


def test_findfiles():
    input = Path("input_folder", "test_file.csv")

    event = parse.findfiles(input)
    event = list(event)
    assert event[0].path == Path(input)
    assert event[1].path == Path(input)


def test_add_filename():
    input = Path("input_folder", "test_file.csv")

    stream = parse.add_filename(
        [
            events.StartContainer(path=Path(input)),
        ]
    )
    stream = list(stream)
    assert stream[0].filename == "test_file"


@patch("builtins.open", create=True)
def test_parse_csv(mock_data):
    input = tmp.gettempdir()
    data = tablib.Dataset(headers=["header_one", "header_two"])
    data.append(["R1C1", "R1C2"])
    data.append(["R2C1", "R2C2"])
    data = data.export("csv")

    mock_data.return_value = data

    stream = parse.parse_csv(
        [
            events.StartContainer()
        ],
        input
    )
    stream = list(stream)
    for e in stream:
        print(e, "---", e.as_dict())
