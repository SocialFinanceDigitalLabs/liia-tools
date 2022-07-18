from liiatools.datasets.cin_census.lds_cin_clean import file_creator

import pandas as pd
from datetime import datetime
import tempfile as tmp


def test_get_year():
    input = r"test\path\cin-2022.xml"
    la_log_dir = tmp.gettempdir()

    data = {"CHILD ID": [123, 456], "DOB": [datetime(2019, 4, 15).date(), datetime(2015, 7, 19).date()]}
    data = pd.DataFrame(data=data)

    stream = file_creator.get_year(input, data, la_log_dir=la_log_dir)
    assert stream["YEAR"].tolist() == ["2022", "2022"]
