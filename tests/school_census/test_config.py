import pytest

from liiatools.school_census_pipeline.spec import load_schema, Term


def test_load_schema():
    schema = load_schema(2017, Term.OCT.value)
    assert schema.table["addressesoffroll"]["addressesoffrolltableid"]


def test_too_early():
    with pytest.raises(ValueError):
        load_schema(2015, Term.OCT.value)


def test_diff_applied():
    schema_2018 = load_schema(2018, Term.JAN.value)
    schema_2020 = load_schema(2019, Term.JAN.value)

    codes_2018 = {x.code for x in schema_2018.table["pupilonroll"]["eyppbf"].category}
    codes_2019 = {x.code for x in schema_2020.table["pupilonroll"]["eyppbf"].category}

    assert len(codes_2018) == 4
    assert len(codes_2019) == 4

    # Removed codes
    assert codes_2018 - codes_2019 == {"EB", "EE", "EO", "EU"}

    # Added codes
    assert codes_2019 - codes_2018 == {"RB", "RE", "RO", "RU"}


def test_table_added():
    schema_2021 = load_schema(2021, Term.OCT.value)
    schema_2022 = load_schema(2022, Term.OCT.value)

    tables_2021 = set(schema_2021.table.keys())
    tables_2022 = set(schema_2022.table.keys())

    assert len(tables_2021) == len(tables_2022) - 5
    assert tables_2022 - tables_2021 == {
        "approvisiondetailoffroll",
        "approvisiondetailonroll",
        "learnerfamoffroll",
        "learnerfamonroll",
        "programmeaimsoffroll"
    }
