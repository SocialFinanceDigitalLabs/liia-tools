import re
from enum import Enum


def check_year(filename):
    """
    Check a filename to see if it contains a year, if it does, return that year
    Expected year formats within string:
        2022
        14032021
        2017-18
        201819
        2019/20
        1920
        21/22
        21-22

    :param filename: Filename that probably contains a year
    :return: Year within the string
    :raises ValueError: If no year is found
    """
    match = re.search(r"(20)(\d{2})(.{0,3}\d{2})*", filename)
    if match:
        try:
            if len(match.group(3)) == 2:
                year = match.group(1) + match.group(3)
                return year
            if len(match.group(3)) == 3:
                year = match.group(1) + match.group(3)[-2:]
                return year
            if len(match.group(3)) == 4:
                year = match.group(3)
                return year
            if len(match.group(3)) == 5:
                year = match.group(3)[-4:]
                return year
        except TypeError:
            year = match.group(1) + match.group(2)
            return year

    fy_match = re.search(r"(\d{2})(.{0,3}\d{2})(.*)(\d*)", filename)
    if fy_match:
        if (
            len(fy_match.group(2)) == 2
            and int(fy_match.group(2)) == int(fy_match.group(1)) + 1
        ):
            year = "20" + fy_match.group(2)
            return year
        if (
            len(fy_match.group(2)) == 3
            and int(fy_match.group(2)[-2:]) == int(fy_match.group(1)) + 1
        ):
            year = "20" + fy_match.group(2)[-2:]
            return year
        if int(fy_match.group(3)[1:3]) == int(fy_match.group(2)[-2:]) + 1:
            year = "20" + fy_match.group(3)[1:3]
            return year
        if int(fy_match.group(2)[-2:]) == int(fy_match.group(2)[-4:-2]) + 1:
            year = "20" + fy_match.group(2)[-2:]
            return year

    raise ValueError


class Term(Enum):
    OCTOBER = "Autumn"
    JANUARY = "Spring"
    MAY = "Summer"


def check_term(filename):
    """
    Check a filename to see if it contains the term information: Autumn/Spring/Summer, if it does, return that term
    Expected filename formats:
        October_2023_addressesonroll.csv
        January_2023_addressesoffroll.csv
    :param filename: Filename that contains a term
    :return: A tern within the string
    :raises ValueError: If no term is found
    """
    match = re.search(
        f"{Term.OCTOBER.name}|{Term.JANUARY.name}|{Term.MAY.name}",
        filename,
        re.IGNORECASE,
    )
    if match:
        return Term[match.group(0).upper()].value

    raise ValueError
