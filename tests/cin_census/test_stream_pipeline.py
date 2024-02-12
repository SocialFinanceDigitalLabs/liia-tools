from fs import open_fs
import xml.etree.ElementTree as ET
import os

from liiatools.cin_census_pipeline.spec import load_schema, load_schema_path
from liiatools.cin_census_pipeline.spec.samples import CIN_2022
from liiatools.cin_census_pipeline.spec.samples import DIR as SAMPLES_DIR
from liiatools.cin_census_pipeline.stream_pipeline import task_cleanfile
from liiatools.common.data import FileLocator


def test_task_cleanfile():
    samples_fs = open_fs(SAMPLES_DIR.as_posix())
    locator = FileLocator(samples_fs, CIN_2022.name)

    result = task_cleanfile(
        locator, schema=load_schema(2022), schema_path=load_schema_path(2022)
    )

    data = result.data
    errors = result.errors

    assert len(data) == 1
    assert len(data["CIN"]) == 10
    assert len(data["CIN"].columns) == 33

    assert len(errors) == 0


def test_task_cleanfile_error():
    tree = ET.parse(CIN_2022)
    root = tree.getroot()

    parent = root.find(".//Source")
    el = parent.find("DateTime")
    el.text = el.text.replace("2022-05-23T11:14:05", "not_date")

    tree.write(SAMPLES_DIR / "cin_2022_error.xml")

    samples_fs = open_fs(SAMPLES_DIR.as_posix())
    locator = FileLocator(samples_fs, "cin_2022_error.xml")

    result = task_cleanfile(
        locator, schema=load_schema(2022), schema_path=load_schema_path(2022)
    )

    data = result.data
    errors = result.errors

    assert len(data) == 1
    assert len(data["CIN"]) == 10
    assert len(data["CIN"].columns) == 33

    assert errors[0]["type"] == "ConversionError"
    assert errors[0]["message"] == "Could not convert to date"
    assert errors[0]["exception"] == "Invalid date: not_date"
    assert errors[0]["filename"] == "cin_2022_error.xml"
    assert errors[0]["header"] == "DateTime"

    os.remove(SAMPLES_DIR / "cin_2022_error.xml")
