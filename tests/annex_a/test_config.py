import pytest

from liiatools.annex_a_pipeline.spec import load_schema


def test_load_schema():
    schema = load_schema(2020)
    assert schema.table["List 1"]["Child Unique ID"]


def test_too_early():
    with pytest.raises(ValueError):
        load_schema(2016)
