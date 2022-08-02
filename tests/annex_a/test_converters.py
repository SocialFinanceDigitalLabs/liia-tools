from liiatools.datasets.annex_a.lds_annexa_clean.converters import to_integer


def test_conversion_to_integer():
    assert to_integer("1") == 1
    assert to_integer("-1") == -1
    assert to_integer("") == ""
