from fs import open_fs

from liiatools.cin_census_pipeline.spec import load_schema
from liiatools.cin_census_pipeline.spec.samples import CIN_2022
from liiatools.cin_census_pipeline.spec.samples import DIR as SAMPLES_DIR
from liiatools.cin_census_pipeline.stream_pipeline import task_cleanfile
from liiatools.common.data import DataContainer, FileLocator, ProcessResult


def test_task_cleanfile():
    samples_fs = open_fs(SAMPLES_DIR.as_posix())
    locator = FileLocator(samples_fs, CIN_2022.name)

    data = task_cleanfile(locator, schema=load_schema(2022))

    assert len(data) == 10
    assert len(data.headers) == 34
