import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

import liiatools
from liiatools.__main__ import cli
from liiatools.csww_pipeline.spec.samples import CSWW_2022


@pytest.fixture(scope="session", autouse=True)
def liiatools_dir():
    return Path(liiatools.__file__).parent


@pytest.fixture(scope="session", autouse=True)
def build_dir(liiatools_dir):
    build_dir = liiatools_dir / "../build/tests/csww"
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

    shutil.copy(CSWW_2022, incoming_dir / f"social_work_workforce_2022.xml")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "csww",
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
