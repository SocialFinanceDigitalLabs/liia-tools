from urllib.error import URLError

import pytest

from liiatools.cin_census_pipeline.spec import load_schema


def test_schema():
    schema = load_schema(2022)
    assert schema.name == "CIN_schema_2022.xsd"

    schema = load_schema(2017)
    assert schema.name == "CIN_schema_2017.xsd"

    with pytest.raises(URLError):
        load_schema(2015)
