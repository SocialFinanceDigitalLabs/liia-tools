import tempfile as tmp
import unittest
from unittest.mock import patch
import pandas as pd

from liiatools.datasets.shared_functions import prep


class TestCheckBlankFile(unittest.TestCase):
    @patch("liiatools.datasets.s903.lds_ssda903_clean.prep.pd.read_csv")
    def test_check_blank_file(self, mock_read_csv):
        la_log_dir = tmp.gettempdir()
        mock_read_csv.side_effect = pd.errors.EmptyDataError

        assert prep.check_blank_file("temp.csv", la_log_dir=la_log_dir) == "empty"


class TestDropEmptyRows(unittest.TestCase):
    @patch("liiatools.datasets.s903.lds_ssda903_clean.prep.pd.read_csv")
    def test_drop_empty_rows(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            {
                "header_one": ["12", "11", None, "14"],
                "header_two": ["yes", "no", None, "maybe"],
            }
        )

        data = prep.drop_empty_rows("temp.csv", "temp.csv")
        assert len(data.index) == 3
