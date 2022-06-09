import pandas as pd

from liiatools.datasets.annex_a.lds_annexa_la_agg import (
    configuration,
    process,
)

list_dict = {"Child Unique ID": [], "Gender": [], "Ethnicity": [], "Date of Birth": [], "Age of Child (Years)": [], "Date of Contact": [], "Contact Source": [], "LA": []}

def test_sort_dict():
    sort_order = configuration.Config()['sort_order']
    test_dict = {"List 1": pd.DataFrame(list_dict)}
    assert process.sort_dict(test_dict, sort_order) == test_dict