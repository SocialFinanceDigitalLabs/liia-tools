import pytest

from liiatools.s251_pipeline.spec import SCHEMA_DIR, load_schema


def test_load_schema():
    schema = load_schema(2023)
    assert schema.table["placement_costs"]["Child ID"]


def test_too_early():
    with pytest.raises(ValueError):
        load_schema(2016)
