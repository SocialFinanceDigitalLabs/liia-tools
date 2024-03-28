import pytest

from liiatools.ssda903_pipeline.spec import SCHEMA_DIR, load_schema


def test_load_schema():
    schema = load_schema(2017)
    assert schema.table["AD1"]["CHILD"]


def test_too_early():
    with pytest.raises(ValueError):
        load_schema(2016)


def test_diff_applied():
    schema_2018 = load_schema(2018)
    schema_2020 = load_schema(2020)

    codes_2018 = {x.code for x in schema_2018.table["Episodes"]["REC"].category}
    codes_2020 = {x.code for x in schema_2020.table["Episodes"]["REC"].category}

    assert len(codes_2018) == 20
    assert len(codes_2020) == 22

    # Removed codes
    assert codes_2018 - codes_2020 == {"E43", "E44"}

    # Added codes
    assert codes_2020 - codes_2018 == {"E45", "E46", "E47", "E48"}


def test_table_added():
    schema_2020 = load_schema(2020)
    schema_2026 = load_schema(2026)

    tables_2020 = set(schema_2020.table.keys())
    tables_2026 = set(schema_2026.table.keys())

    assert len(tables_2020) == len(tables_2026) - 1
    assert tables_2026 - tables_2020 == {
        "SocialWorker",
    }
