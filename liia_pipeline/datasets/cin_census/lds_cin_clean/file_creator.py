from pathlib import Path
import re

def get_year(input, data):
    filename = Path(input).stem
    match = re.search(r"20\d{2}", filename)
    year = match.group(0)
    data['YEAR'] = year
    return data

def get_person_school_year(data):
    if data['PersonBirthDate'].dt.month >= 9:
        data['PersonSchoolYear'] = data['PersonBirthDate'].dt.year
    elif data['PersonBirthDate'].dt.month <= 8:
        data['PersonSchoolYear'] = data['PersonBirthDate'].dt.year - 1
    else:
        data['PersonSchoolYear'] = None
    return data

def add_la_name(data, la_name):
    data['LA'] = la_name
    return data

def add_fields(input, data, la_name):
    '''Adds YEAR, LA, PERSONSCHOOLYEAR to exported dataframe
       Appends LA_code from config to LAChildID'''

    data = get_year(input, data)
    data = get_person_school_year(data)
    data = add_la_name(data, la_name)
    return data

def export_file(input, output, data):
        filename = Path(input).stem
        outfile = filename + "_clean.csv"
        output_path = Path(output, outfile)
        data.to_csv(output_path, index=False)