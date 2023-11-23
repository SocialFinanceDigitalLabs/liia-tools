from liiatools.datasets.social_work_workforce.lds_csww_clean import converters


def test_to_category():
    category_dict = [
        {"code": "M1"},
        {"code": "F1"},
        {"code": "MM"},
        {"code": "FF"},
        {"code": "MF"},
    ]
    assert converters.to_category("M1", category_dict) == "M1"
    assert converters.to_category("M2", category_dict) == "error"
    assert converters.to_category("MF", category_dict) == "MF"
    assert converters.to_category("", category_dict) == ""
    assert converters.to_category(None, category_dict) == ""

    category_dict = [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
    assert converters.to_category(0, category_dict) == "0"
    assert converters.to_category("false", category_dict) == "0"
    assert converters.to_category(1.0, category_dict) == "1"
    assert converters.to_category("true", category_dict) == "1"
    assert converters.to_category("string", category_dict) == "error"
    assert converters.to_category(123, category_dict) == "error"
    assert converters.to_category("", category_dict) == ""
    assert converters.to_category(None, category_dict) == ""


def test_to_numeric():
    decimal_places = 3
    assert converters.to_numeric("12.345", "decimal", decimal_places) == 12.345
    assert converters.to_numeric("12.3456", "decimal", decimal_places) == 12.346
    assert converters.to_numeric("12.3", "decimal", decimal_places) == 12.3
    assert converters.to_numeric(12.3456, "decimal", decimal_places) == 12.346
    assert converters.to_numeric("1.0", "decimal", decimal_places) == 1
    assert converters.to_numeric(0, "decimal", decimal_places) == 0
    assert converters.to_numeric("date", "") == "date"
    assert converters.to_numeric("", "decimal", decimal_places) == ""
    assert converters.to_numeric(None, "decimal", decimal_places) == ""
    assert (
        converters.to_numeric(
            "0.3", "decimal", decimal_places, min_inclusive=0, max_inclusive=1
        )
        == 0.3
    )
    assert (
        converters.to_numeric("0.3", "decimal", decimal_places, min_inclusive=0) == 0.3
    )
    assert (
        converters.to_numeric("0.3", "decimal", decimal_places, max_inclusive=1) == 0.3
    )
    assert (
        converters.to_numeric(
            "1.99", "decimal", decimal_places, min_inclusive=0, max_inclusive=1
        )
        == "error"
    )
    assert (
        converters.to_numeric(
            "0.3", "decimal", decimal_places, min_inclusive=1, max_inclusive=99
        )
        == "error"
    )
    assert converters.to_numeric("3000", "integer") == 3000
    assert converters.to_numeric(123, "integer") == 123
    assert converters.to_numeric("1.0", "integer") == 1
    assert converters.to_numeric("date", "") == "date"
    assert converters.to_numeric(123.456, "integer") == 123
    assert converters.to_numeric(0, "integer") == 0
    assert converters.to_numeric("", "integer") == ""
    assert converters.to_numeric(None, "integer") == ""


def test_to_regex():
    pattern = r"[A-Za-z]{2}\d{10}"
    assert converters.to_regex("AB1234567890", pattern) == "AB1234567890"  # match
    assert converters.to_regex("  AB1234567890  ", pattern) == "AB1234567890"  # match
    assert converters.to_regex("AB1234567890123456", pattern) == "error"  # too long
    assert converters.to_regex("AB12345", pattern) == "error"  # too short
    assert converters.to_regex("xxxxOz2054309383", pattern) == "error"  # invalid format
    assert converters.to_regex("", pattern) == ""  # no value
    assert converters.to_regex(None, pattern) == ""  # no value
