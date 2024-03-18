import pandas as pd
import pytest
from fs import open_fs

from liiatools.common.data import DataContainer


@pytest.fixture
def sample_data():
    return DataContainer(
        {
            "table1": pd.DataFrame(
                [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
            ),
            "table2": pd.DataFrame(
                [
                    {"id": 1, "date": pd.to_datetime("2022-01-01")},
                    {"id": 2, "date": pd.to_datetime("2022-05-03")},
                ]
            ),
        }
    )


def test_to_dataset(sample_data: DataContainer):
    dataset = sample_data.to_dataset("table1")
    assert dataset
    assert dataset.title == "table1"
    assert dataset.headers == ["id", "name"]


def test_to_databook(sample_data: DataContainer):
    databook = sample_data.to_databook()
    assert databook
    assert len(databook.sheets()) == 2
    assert databook.sheets()[0].title == "table1"
    assert databook.sheets()[1].title == "table2"


def test_export(sample_data: DataContainer):
    fs = open_fs("mem://")
    sample_data.export(fs, "test_", format="csv")

    assert fs.exists("test_table1.csv")
    assert fs.exists("test_table2.csv")

    sample_data.export(fs, "excel_test", format="xlsx")
    assert fs.exists("excel_test.xlsx")


def test_export_parquet(sample_data: DataContainer):
    fs = open_fs("mem://")
    sample_data.export(fs, "test_", format="parquet")

    assert fs.exists("test_table1.parquet")
    assert fs.exists("test_table1.parquet")
