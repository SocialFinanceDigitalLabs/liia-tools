import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

import liiatools
from liiatools.__main__ import cli


@pytest.fixture(scope="session", autouse=True)
def liiatools_dir():
    return Path(liiatools.__file__).parent


@pytest.fixture(scope="session", autouse=True)
def build_dir(liiatools_dir):
    build_dir = liiatools_dir / "../build/tests/s903"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    return build_dir


@pytest.fixture(scope="session", autouse=True)
def log_dir(build_dir):
    log_dir = build_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    return build_dir


def test_end_to_end(liiatools_dir, build_dir, log_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "s903",
            "cleanfile",
            "--i",
            str(liiatools_dir / "spec/s903/samples/SSDA903_2020_episodes.csv"),
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

    result = runner.invoke(
        cli,
        [
            "s903",
            "la-agg",
            "--i",
            str(build_dir / "SSDA903_2020_episodes_clean.csv"),
            "--o",
            str(build_dir),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "s903",
            "pan-agg",
            "--i",
            str(build_dir / "SSDA903_Episodes_merged.csv"),
            "--o",
            str(build_dir),
            "--la_code",
            "BAD",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
