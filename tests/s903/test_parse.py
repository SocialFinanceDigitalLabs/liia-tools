import tempfile as tmp
from pathlib import Path
from unittest.mock import mock_open, patch

import tablib
from sfdata_stream_parser import events

from liiatools.ssda903_pipeline.lds_ssda903_clean import parse

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
