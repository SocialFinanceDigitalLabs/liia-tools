from pathlib import Path
import csv
import os
import tempfile as tmp
import unittest

from liiatools.datasets.s903.lds_ssda903_clean import prep


def test_drop_empty_rows():
    temp_file = Path(r"temp.csv")
    test_header = ["header_one", "header_two"]
    test_data = [
        ["12", "yes"],
        ["11", "no"],
        ["", ""],
        ["14", "maybe"],
    ]
    with open(temp_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(test_header)
        writer.writerows(test_data)

    data = prep.drop_empty_rows(temp_file, temp_file)
    assert len(data.index) == 3
    os.remove(temp_file)


class TestDeleteUnrequiredFiles(unittest.TestCase):
    def test_check_year(self):
        temp_file = Path(r"temp.csv")
        drop_file_list = ["temp"]
        la_log_dir = tmp.gettempdir()

        with open(temp_file, 'w') as file:
            csv.writer(file)

        with self.assertRaises(Exception):
            prep.delete_unrequired_files(temp_file, drop_file_list, la_log_dir)
            assert ~temp_file.is_file()


def test_delete_unrequired_files():
    temp_file = Path(r"temp.csv")
    drop_file_list = ["not_temp"]
    la_log_dir = tmp.gettempdir()

    with open(temp_file, 'w') as file:
        csv.writer(file)

    prep.delete_unrequired_files(temp_file, drop_file_list, la_log_dir)
    assert temp_file.is_file()
    os.remove(temp_file)
