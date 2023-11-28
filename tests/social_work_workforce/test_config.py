import pytest
from urllib.error import URLError

from liiatools.csww_pipeline.spec import SCHEMA_DIR, load_schema


def test_load_schema():
    schema = load_schema(2022)
    assert schema.name == "social_work_workforce_schema_2022.xsd"


def test_too_early():
    with pytest.raises(URLError):
        load_schema(2016)
