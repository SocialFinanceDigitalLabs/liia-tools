from datetime import datetime
from sfdata_stream_parser import events
from liiatools.datasets.social_work_workforce.lds_csww_clean import cleaner


def test_clean_dates():
    event = events.TextNode(text=datetime(2019, 1, 15), schema_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.text == datetime(2019, 1, 15).date()
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="2019/1/15", schema_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(
        text=datetime(2019, 1, 15), schema_dict={"not_date": "%d/%m/%Y"}
    )
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.text == datetime(2019, 1, 15)

    event = events.TextNode(text="string", schema_dict={"not_date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.text == "string"

    event = events.TextNode(text=None, schema_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="", schema_dict={"date": "%d/%m/%Y"})
    cleaned_event = list(cleaner.clean_dates(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"


def test_clean_categories():
    event = events.TextNode(
        text="0",
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == "0"
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(
        text="0.0",
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == "0"
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(
        text=0,
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == "0"
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(
        text="true",
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == "1"
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(
        text=123,
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(
        text="string",
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(
        text="string",
        schema_dict={
            "not_category": [
                {"code": "0", "name": "False"},
                {"code": "1", "name": "True"},
            ]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == "string"

    event = events.TextNode(
        text=None,
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(
        text="",
        schema_dict={
            "category": [{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
        },
    )
    cleaned_event = list(cleaner.clean_categories(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"


def test_clean_numeric():
    event = events.TextNode(text=123, schema_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="", schema_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text=None, schema_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="123", schema_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="string", schema_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(text=datetime(2017, 3, 17), schema_dict={"numeric": "integer"})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(
        text=datetime(2017, 3, 17), schema_dict={"not_numeric": "integer"}
    )
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == datetime(2017, 3, 17)

    event = events.TextNode(text=123.45, schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123.45
    assert cleaned_event.formatting_error == "0"
    
    event = events.TextNode(text=123.4567, schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123.46
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text=123.45, schema_dict={"numeric": "decimal", "decimal": 0})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text=123.456, schema_dict={"numeric": "decimal", "decimal": 6})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123.456
    assert cleaned_event.formatting_error == "0"
    
    event = events.TextNode(text="", schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text=None, schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="123.4567", schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 123.46
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="0.45", schema_dict={"numeric": "decimal", "decimal": 2, "min_inclusive": 0, "max_inclusive": 1})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == 0.45
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="1.99", schema_dict={"numeric": "decimal", "decimal": 2, "min_inclusive": 0, "max_inclusive": 1})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1" # exceeds maximum value

    event = events.TextNode(text="0.50", schema_dict={"numeric": "decimal", "decimal": 2, "min_inclusive": 1, "max_inclusive": 9})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1" # less than minimum value

    event = events.TextNode(text="string", schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1" # not a decimal

    event = events.TextNode(text=datetime(2017, 3, 17), schema_dict={"numeric": "decimal", "decimal": 2})
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1" # not a decimal

    event = events.TextNode(
        text=datetime(2017, 3, 17), schema_dict={"not_numeric": "decimal"}
    )
    cleaned_event = list(cleaner.clean_numeric(event))[0]
    assert cleaned_event.text == datetime(2017, 3, 17)

def test_clean_regex_string():
    event = events.TextNode(text="AB1234567890", schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == "AB1234567890"
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="", schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text=None, schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="      AB1234567890    ", schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == "AB1234567890"
    assert cleaned_event.formatting_error == "0"

    event = events.TextNode(text="AB123456", schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(text="AB1234567890123456", schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(text="AB12345 67890", schema_dict={"regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == ""
    assert cleaned_event.formatting_error == "1"

    event = events.TextNode(text="string", schema_dict={"not_regex_string": r"[A-Za-z]{2}\d{10}"})
    cleaned_event = list(cleaner.clean_regex_string(event))[0]
    assert cleaned_event.text == "string"


# test_clean_dates()
# test_clean_categories()
# test_clean_numeric()
# test_clean_regex_string()

