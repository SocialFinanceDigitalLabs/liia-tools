from pathlib import Path
import tempfile as tmp
from unittest.mock import patch, mock_open
import tablib

from liiatools.datasets.shared_functions import parse

from sfdata_stream_parser import events


# @patch("builtins.open", create=True)
# def test_parse_csv(mock_data):
#     input = tmp.gettempdir()
#     data = tablib.Dataset(headers=["header_one", "header_two"])
#     data.append(["R1C1", "R1C2"])
#     data.append(["R2C1", "R2C2"])
#     data = data.export("csv")
#
#     mock_data.return_value = data
#
#     stream = parse.parse_csv(
#         input
#     )
#     stream = list(stream)
#     for e in stream:
#         print(e, "---", e.as_dict())
