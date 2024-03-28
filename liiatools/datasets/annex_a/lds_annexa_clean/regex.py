import re
from typing import Pattern

__flag_resolved = dict(i=re.I, m=re.M, s=re.S, u=re.U, l=re.L, x=re.X)


def resolve_flags(flags: str) -> int:
    flag_expr = 0
    if flags is not None:
        for flag in flags:
            flag_expr = flag_expr | __flag_resolved[flag]
    return flag_expr


def parse_regex(regex: str) -> Pattern:
    """
    Parse a regex pattern '/{pattern}/{modifiers}'

    :param regex: regex expression
    :return: Compiled regex
    """
    separator = re.escape(regex[0])
    pattern = re.compile(
        "{separator}(.+)({separator}([imsulx]+)?)".format(separator=separator)
    )
    match = pattern.match(regex)
    if match is None:
        raise Exception("Failed to parse regular expression: '{}'".format(regex))

    pattern = match.group(1)
    flags = match.group(3)
    flags = resolve_flags(flags)

    return re.compile(pattern, flags)
