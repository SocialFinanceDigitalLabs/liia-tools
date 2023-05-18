from liiatools.datasets.cin_census.lds_cin_clean import file_creator

import pandas as pd
from datetime import datetime
import tempfile as tmp


def test_get_year():
    year = "2022"

    data = {
        "CHILD ID": [123, 456],
        "DOB": [datetime(2019, 4, 15).date(), datetime(2015, 7, 19).date()],
    }
    data = pd.DataFrame(data=data)

    stream = file_creator.get_year(data, year)
    assert stream["YEAR"].tolist() == ["2022", "2022"]
