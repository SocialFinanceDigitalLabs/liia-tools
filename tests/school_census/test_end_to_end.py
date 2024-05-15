import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

import liiatools
from liiatools.__main__ import cli
from liiatools.school_census_pipeline.spec.samples import SCHOOL_CENSUS


@pytest.fixture(scope="session", autouse=True)
def liiatools_dir():
    return Path(liiatools.__file__).parent


@pytest.fixture(scope="session", autouse=True)
def build_dir(liiatools_dir):
    build_dir = liiatools_dir / "../build/tests/school_census"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    return build_dir


@pytest.mark.skipif(os.environ.get("SKIP_E2E"), reason="Skipping end-to-end tests")
def test_end_to_end(build_dir):
    incoming_dir = build_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir = build_dir / "pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2020, 2024):
        year_dir = incoming_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(SCHOOL_CENSUS, year_dir / f"Oct_{year}_addressesonroll.csv")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "s903",
            "pipeline",
            "-c",
            "BAD",
            "--input",
            incoming_dir.as_posix(),
            "--output",
            pipeline_dir.as_posix(),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0


@pytest.mark.skip("Old pipeline")
def test_end_to_end_old(liiatools_dir, build_dir, log_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "school-census",
            "cleanfile",
            "--i",
            str(
                liiatools_dir
                / "school_census_pipeline/spec/samples/addressesonroll.csv"
            ),
            "--o",
            str(build_dir),
            "--la_log_dir",
            str(log_dir),
            "--la_code",
            "BAD",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
