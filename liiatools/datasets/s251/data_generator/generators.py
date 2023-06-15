import random
from datetime import datetime, timedelta
from liiatools.datasets.s251.configuration import Config

config = Config()


def generate_child_id() -> int:
    """
    Generator for child IDs (an integer between 0 and 1 million).
    Uniqueness is guaranteed for the life of the generator.

    :returns: A new child id
    """
    seen_child_ids = set()
    while True:
        child_id = random.randint(0, 1_000_000)
        if child_id not in seen_child_ids:
            seen_child_ids.add(child_id)
            yield child_id


def generate_dob(reference_date: datetime, age_start: int = 0, age_end: int = 18) -> datetime:
    """
    Given a reference date, this will generate a date of birth such that the age is between age_start and age_end.
    The age is sampled uniformly from the range.

    :param reference_date: Date at which age_start, age_end applies.
    :param age_start: Minimal age at reference_date
    :param age_end: Maximal age at reference_date
    :returns: Generated date of birth.
    """
    age_at_start_in_days = random.randint(365 * age_start, 365 * age_end)
    return reference_date - timedelta(days=age_at_start_in_days)


def _list_from_schema(attribute: str) -> list:
    """
    Generate a list of available categories for a given attribute from the schema.

    :param attribute: Name of the attribute e.g. Gender, Ethnicity.
    :return: List of available attribute codes from the schema.
    """
    return list(value["code"] for value in config["placement_costs"][attribute]["category"])


def generate_attribute(attribute: str) -> str:
    """
    Randomly samples a given attribute from the list of all available attribute codes in the schema.

    :param attribute: Name of the attribute e.g. Gender, Ethnicity.
    :returns: Attribute string e.g. 1 for Gender, WBRI for Ethnicity.
    """
    return random.choice(_list_from_schema(attribute))


def generate_date_of_last_assessment(reference_date: datetime, dob: datetime) -> datetime:
    """
    Given a date of birth, this will generate a date of last assessment such that the assessment is between the date of
    birth and the date today.
    The date is sampled uniformly from the range.

    :param reference_date:
    :param dob: Date of birth.
    :return: Generated date of last assessment.
    """
    days_before_reference = (reference_date - dob).days
    date_of_last_assessment = dob + timedelta(days=random.uniform(0, days_before_reference))
    return date_of_last_assessment

