import pandas as pd
import pytest
from fs import open_fs

from liiatools.common.archive import DataframeArchive
from liiatools.common.data import ColumnConfig, PipelineConfig, TableConfig


@pytest.fixture
def cfg():
    cfg = PipelineConfig(
        table_list=[
            TableConfig(
                id="table1",
                columns=[
                    ColumnConfig(id="id", type="integer", unique_key=True),
                    ColumnConfig(id="name", type="string"),
                ],
            ),
            TableConfig(
                id="table2",
                columns=[
                    ColumnConfig(id="id", type="integer", unique_key=True),
                    ColumnConfig(id="date", type="date"),
                ],
            ),
        ]
    )
    return cfg


@pytest.fixture(scope="function")
def fs():
    fs = open_fs("mem://")
    return fs


@pytest.fixture
def archive(fs, cfg: PipelineConfig) -> DataframeArchive:
    archive = DataframeArchive(fs, cfg, "20240430T092230.112993")
    return archive


def test_archive(archive: DataframeArchive):
    la_code = "BAR"
    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]),
        "table2": pd.DataFrame(
            [
                {"id": 1, "date": pd.to_datetime("2022-01-01")},
                {"id": 2, "date": pd.to_datetime("2022-05-03")},
            ]
        ),
    }
    session = archive.add(dataset, la_code)
    assert session

    snapshots = archive.list_snapshots()
    assert snapshots == {la_code: [session]}

    snap = archive.load_snapshot(session)
    assert snap["table1"].shape == (2, 2)
    assert snap["table1"]["name"].tolist() == ["foo", "bar"]

    assert snap.keys() == dataset.keys()


def test_combine(archive: DataframeArchive):
    la_code = "BAR"
    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]),
    }
    archive.add(dataset, la_code)

    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "Foo"}, {"id": 3, "name": "FooBar"}]),
    }
    archive.add(dataset, la_code)

    dataset = {
        "table1": pd.DataFrame([{"id": 4, "name": "SNAFU"}]),
    }
    archive.add(dataset, la_code)

    snapshots = archive.list_snapshots()
    assert len(snapshots[la_code]) == 3

    combined = archive.current(la_code)
    table_1 = combined["table1"]

    assert sorted(table_1.id.tolist()) == [1, 2, 3, 4]
    assert sorted(table_1.name.tolist()) == sorted(["Foo", "bar", "FooBar", "SNAFU"])


def test_rollup(archive: DataframeArchive):
    la_code = "BAR"
    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]),
    }
    archive.add(dataset, la_code)

    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "Foo"}, {"id": 3, "name": "FooBar"}]),
    }
    archive.add(dataset, la_code)

    # We have now archived two snapshots
    assert len(archive.list_snapshots()[la_code]) == 2

    # And the current view should be a combination of the two
    table_1 = archive.current(la_code)["table1"]
    assert sorted(table_1.id.tolist()) == [1, 2, 3]
    assert sorted(table_1.name.tolist()) == sorted(["Foo", "bar", "FooBar"])

    # Let's create a rollup
    archive.rollup()

    # We have another snapshot
    assert len(archive.list_snapshots()[la_code]) == 3

    # Let's add some more data
    dataset = {
        "table1": pd.DataFrame([{"id": 4, "name": "SNAFU"}]),
    }
    archive.add(dataset, la_code)

    # Let's delete the snapshots prior to the rollup
    snapshot_list = archive.list_snapshots()[la_code]
    archive.delete_snapshot(*snapshot_list[:2])

    # We only have two left
    assert len(archive.list_snapshots()[la_code]) == 2

    # But the combined view still reflects the data from those snapshots
    table_1 = archive.current(la_code)["table1"]
    assert sorted(table_1.id.tolist()) == [1, 2, 3, 4]
    assert sorted(table_1.name.tolist()) == sorted(["Foo", "bar", "FooBar", "SNAFU"])

    # We can't accidentally delete the rollup
    with pytest.raises(Exception):
        archive.delete_snapshot(*snapshot_list)

    # But we can delete it if we want to - but let's create a new one first
    archive.rollup()
    old_snaps = archive.list_snapshots()[la_code][:-1]
    archive.delete_snapshot(*old_snaps, allow_rollups=True)

    # Only one file left
    assert len(archive.list_snapshots()[la_code]) == 1

    # But all the data
    table_1 = archive.current(la_code)["table1"]
    assert sorted(table_1.id.tolist()) == [1, 2, 3, 4]
    assert sorted(table_1.name.tolist()) == sorted(["Foo", "bar", "FooBar", "SNAFU"])
