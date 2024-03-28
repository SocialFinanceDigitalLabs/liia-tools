from pathlib import Path
import csv
import os
import tempfile as tmp
import unittest

from liiatools.datasets.s903.lds_ssda903_clean import prep


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
