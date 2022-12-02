from pathlib import Path
import csv
import os
import tempfile as tmp
import unittest
from unittest.mock import patch
import pandas as pd

from liiatools.datasets.s903.lds_ssda903_clean import prep


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


class TestDeleteUnrequiredFiles(unittest.TestCase):
    def test_check_year(self):
        temp_file = "temp.csv"
        drop_file_list = ["temp"]
        la_log_dir = tmp.gettempdir()

        with open(temp_file, "w") as file:
            csv.writer(file)

        with self.assertRaises(Exception):
            prep.delete_unrequired_files(temp_file, drop_file_list, la_log_dir)
        assert ~Path(temp_file).is_file()


def test_delete_unrequired_files():
    temp_file = "temp.csv"
    drop_file_list = ["not_temp"]
    la_log_dir = tmp.gettempdir()

    with open(temp_file, "w") as file:
        csv.writer(file)

    prep.delete_unrequired_files(temp_file, drop_file_list, la_log_dir)
    assert Path(temp_file).is_file()
    os.remove(temp_file)
