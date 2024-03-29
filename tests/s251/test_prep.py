import unittest
from datetime import datetime
from pathlib import Path
import csv

from liiatools.datasets.s251.lds_s251_clean import prep


class TestSaveYearError(unittest.TestCase):
    def test_save_missing_column_error(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data
        input_file = "s251_test.csv"
        la_log_dir = str(temp_dir)
        data_type = prep.DataType.MISSING_COLUMN

        # Call the function to be tested
        prep._save_year_error(input_file, la_log_dir, data_type)

        # Generate expected output
        filename = Path(input_file).resolve().stem
        expected_output = (
            f"Could not process '{filename}' because end date column was not found which is used to "
            f"identify the year of return"
        )

        # Read the content of the generated error log file
        error_log_file = (
            temp_dir / f"{filename}_error_log_{datetime.now():%Y-%m-%dT%H%M%SZ}.txt"
        )
        with open(error_log_file, "r") as f:
            actual_output = f.read().strip()

        # Assertions
        self.assertEqual(actual_output, expected_output)

    def test_save_empty_column_error(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data
        input_file = "s251_test.csv"
        la_log_dir = str(temp_dir)
        data_type = prep.DataType.EMPTY_COLUMN

        # Call the function to be tested
        prep._save_year_error(input_file, la_log_dir, data_type)

        # Generate expected output
        filename = Path(input_file).resolve().stem
        expected_output = (
            f"Could not process '{filename}' because end date column was empty which is used to "
            f"identify the year of return"
        )

        # Read the content of the generated error log file
        error_log_file = (
            temp_dir / f"{filename}_error_log_{datetime.now():%Y-%m-%dT%H%M%SZ}.txt"
        )
        with open(error_log_file, "r") as f:
            actual_output = f.read().strip()

        # Assertions
        self.assertEqual(actual_output, expected_output)

    def test_save_old_data_error(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data
        input_file = "s251_test.csv"
        la_log_dir = str(temp_dir)
        data_type = prep.DataType.OLD_DATA

        # Call the function to be tested
        prep._save_year_error(input_file, la_log_dir, data_type, year=2021)

        # Generate expected output
        filename = Path(input_file).resolve().stem
        expected_output = (
            f"Could not process '{filename}' because this file is from the year 2021 and we are only accepting "
            f"data from 2023 onwards"
        )

        # Read the content of the generated error log file
        error_log_file = (
            temp_dir / f"{filename}_error_log_{datetime.now():%Y-%m-%dT%H%M%SZ}.txt"
        )
        with open(error_log_file, "r") as f:
            actual_output = f.read().strip()

        # Assertions
        self.assertEqual(actual_output, expected_output)

    def tearDown(self):
        # Clean up temporary directory after each test
        temp_dir = Path("temp_logs")
        for file in temp_dir.glob("*.txt"):
            file.unlink()
        temp_dir.rmdir()


class TestFindYearOfReturn(unittest.TestCase):
    def test_find_year_of_return_successful(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data: Create a CSV file with placement end dates
        csv_data = [
            ["Placement end date"],
            ["15/07/2023"],
            ["31/12/2022"],
            ["not a date"],
            ["15/02/3022"],
        ]
        input_file = "s251_test.csv"
        input_file_path = temp_dir / input_file
        with open(input_file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)

        # Call the function to be tested
        year, financial_year, quarter = prep.find_year_of_return(
            str(input_file_path), str(temp_dir), retention_period=6, reference_year=2023
        )

        # Assertions
        self.assertEqual(year, 2022)
        self.assertEqual(financial_year, 2023)
        self.assertEqual(quarter, "Q3")

    def test_find_year_of_return_missing_column(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data: Create a CSV file without the Placement end date column
        csv_data = [
            ["Other column"],
            ["15/07/2023"],
            ["31/12/2022"],
        ]
        input_file = "s251_test.csv"
        input_file_path = temp_dir / input_file
        with open(input_file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)

        # Call the function to be tested
        year, financial_year, quarter = prep.find_year_of_return(
            str(input_file_path), str(temp_dir), retention_period=6, reference_year=2023
        )

        # Assertions
        self.assertIsNone(year)
        self.assertIsNone(financial_year)
        self.assertIsNone(quarter)

        # Verify the error log file is created with the correct message
        error_log_file = (
            temp_dir / f"s251_test_error_log_{datetime.now():%Y-%m-%dT%H%M%SZ}.txt"
        )
        with open(error_log_file, "r") as f:
            error_message = f.read()
            self.assertIn(
                "Could not process 's251_test' because end date column was not found which "
                "is used to identify the year of return",
                error_message,
            )

    def test_find_year_of_return_empty_column(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data: Create a CSV file with an empty Placement end date column
        csv_data = [
            ["Placement end date"],
            [""],
            [""],
        ]
        input_file = "s251_test.csv"
        input_file_path = temp_dir / input_file
        with open(input_file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)

        # Call the function to be tested
        year, financial_year, quarter = prep.find_year_of_return(
            str(input_file_path), str(temp_dir), retention_period=6, reference_year=2023
        )

        # Assertions
        self.assertIsNone(year)
        self.assertIsNone(financial_year)
        self.assertIsNone(quarter)

        # Verify the error log file is created with the correct message
        error_log_file = (
            temp_dir / f"s251_test_error_log_{datetime.now():%Y-%m-%dT%H%M%SZ}.txt"
        )
        with open(error_log_file, "r") as f:
            error_message = f.read()
            self.assertIn(
                "Could not process 's251_test' because end date column was empty which is used to "
                "identify the year of return",
                error_message,
            )

    def test_find_year_of_return_old_data(self):
        # Create a temporary directory for testing
        temp_dir = Path("temp_logs")
        temp_dir.mkdir(exist_ok=True)

        # Test data: Create a CSV file with Placement a end date between 2017-2022
        csv_data = [
            ["Placement end date"],
            ["15/07/2023"],
            ["31/12/2021"],
        ]
        input_file = "s251_test.csv"
        input_file_path = temp_dir / input_file
        with open(input_file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)

        # Call the function to be tested
        year, financial_year, quarter = prep.find_year_of_return(
            str(input_file_path), str(temp_dir), retention_period=6, reference_year=2023
        )

        # Assertions
        self.assertIsNone(year)
        self.assertIsNone(financial_year)
        self.assertIsNone(quarter)

        # Verify the error log file is created with the correct message
        error_log_file = (
            temp_dir / f"s251_test_error_log_{datetime.now():%Y-%m-%dT%H%M%SZ}.txt"
        )
        with open(error_log_file, "r") as f:
            error_message = f.read()
            self.assertIn(
                "Could not process 's251_test' because this file is from the year 2021 and we are only accepting "
                "data from 2023 onwards",
                error_message,
            )

    def tearDown(self):
        # Clean up temporary directory after each test
        temp_dir = Path("temp_logs")
        for file in temp_dir.glob("*"):
            file.unlink()
        temp_dir.rmdir()
