from liiatools.datasets.shared_functions import converters


def degrade_dob(data):
    if data["PersonBirthDate"] is not None:
        data["PersonBirthDate"] = converters.to_month_only_dob(data["PersonBirthDate"])
    return data


def degrade_expected_dob(data):
    if data["ExpectedPersonBirthDate"] is not None:
        data["ExpectedPersonBirthDate"] = converters.to_month_only_dob(
            data["ExpectedPersonBirthDate"]
        )
    return data


def degrade_death_date(data):
    if data["PersonDeathDate"] is not None:
        data["PersonDeathDate"] = converters.to_month_only_dob(data["PersonDeathDate"])
    return data


def add_fields(input, data, la_name):
    """Adds YEAR, LA, PERSONSCHOOLYEAR to exported dataframe
    Appends LA_code from config to LAChildID"""
    data = degrade_dob(data)
    data = degrade_expected_dob(data)
    data = degrade_death_date(data)
    return data
