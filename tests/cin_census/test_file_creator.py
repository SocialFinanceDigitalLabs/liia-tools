from liiatools.datasets.cin_census.lds_cin_clean import file_creator

import tablib
from datetime import datetime


def test_get_year():
    input = r"test\path\cin-2022.xml"

    headers = ["CHILD ID", "DOB"]
    row = [12345, datetime(2019, 4, 15).date()]
    data = tablib.Dataset(headers=headers)
    data.append(row)

    stream = file_creator.get_year(input, data)
    assert stream["YEAR"] == ["2022"]


def test_get_person_school_year():
    headers = ["CHILD ID", "DOB", "PersonBirthDate"]
    row = [12345, datetime(2019, 4, 15).date(), datetime(2019, 4, 15).date()]
    data = tablib.Dataset(headers=headers)
    data.append(row)

    stream = file_creator.get_person_school_year(data)
    print(stream)
