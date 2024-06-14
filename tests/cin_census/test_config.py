from urllib.error import URLError

import pytest

from liiatools.cin_census_pipeline.spec import load_xml_schema, load_csv_schema


def test_schema():
    schema = load_xml_schema(2022)
    assert schema.name == "CIN_schema_2022.xsd"

    schema = load_xml_schema(2017)
    assert schema.name == "CIN_schema_2017.xsd"

    with pytest.raises(URLError):
        load_xml_schema(2015)


def test_load_schema():
    schema = load_csv_schema(2017)
    assert schema.table["assessments"]["assessmentstableid"]


def test_too_early():
    with pytest.raises(ValueError):
        load_csv_schema(2015)


def test_diff_applied():
    schema_2022 = load_csv_schema(2022)
    schema_2023 = load_csv_schema(2023)

    codes_2022 = {x.code for x in schema_2022.table["cindetails"]["reasonforclosure"].category}
    codes_2023 = {x.code for x in schema_2023.table["cindetails"]["reasonforclosure"].category}

    assert len(codes_2022) == 8
    assert len(codes_2023) == 9

    # Added codes
    assert codes_2023 - codes_2022 == {"RC9"}
