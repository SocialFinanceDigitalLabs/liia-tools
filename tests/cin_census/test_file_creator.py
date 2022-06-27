from liiatools.datasets.cin_census.lds_cin_clean import file_creator

import pandas as pd
from datetime import datetime


def test_get_year():
    input = r"test\path\cin-2022.xml"

    data = {"CHILD ID": [123, 456], "DOB": [datetime(2019, 4, 15).date(), datetime(2015, 7, 19).date()]}
    data = pd.DataFrame(data=data)

    stream = file_creator.get_year(input, data)
    assert stream["YEAR"].tolist() == ["2022", "2022"]
