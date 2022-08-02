import re

from liiatools.datasets.annex_a.lds_annexa_clean import regex


def test_resolve_flags():
    flag = "i"
    assert regex.resolve_flags(flag) == re.I

    flag = "m"
    assert regex.resolve_flags(flag) == re.M

    flag = "s"
    assert regex.resolve_flags(flag) == re.S

    flag = "u"
    assert regex.resolve_flags(flag) == re.U

    flag = "l"
    assert regex.resolve_flags(flag) == re.L

    flag = "x"
    assert regex.resolve_flags(flag) == re.X


def test_parse_regex():
    string = "/.*unknown.*/i"
    assert regex.parse_regex(string) == re.compile('.*unknown.*', re.IGNORECASE)

    string = "/.*whi.*british.*/m"
    assert regex.parse_regex(string) == re.compile('.*whi.*british.*', re.MULTILINE)
