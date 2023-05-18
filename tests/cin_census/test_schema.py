from liiatools.datasets.cin_census.lds_cin_clean.schema import Schema


def test_schema():

    schema=Schema(2022).schema
    assert schema.name == "CIN_schema_2022.xsd"

    schema=Schema(2017).schema
    assert schema.name == "CIN_schema_2017.xsd"
