from pathlib import Path
import re
import logging

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)


@streamfilter(check=checks.type_check(events.StartElement), fail_function=pass_event)
def add_year(event, input):
    """
    Searches the input for the year by finding any four-digit number starting with 20

    :param event: A filtered list of event objects of type StartElement
    :param input: Location of file to be cleaned
    :return: An updated list of event objects
    """
    filename = Path(input).stem
    try:
        match = re.search(r"20\d{2}", filename)
        year = match.group(0)
        return event.from_event(event, year=year)
    except AttributeError:
        return event.from_event(event, year_error=f"Unable to find year in {filename}")


@streamfilter(check=checks.type_check(events.StartElement), fail_function=pass_event)
def add_la_name(event, la_name):
    """
    Adds the Local Authority name to the StartElement

    :param event: A filtered list of event objects of type StartElement
    :param la_name: Name of the local authority
    :return: An updated list of event objects
    """
    return event.from_event(event, la=la_name)


@streamfilter(check=checks.type_check(events.TextNode), fail_function=pass_event)
def create_la_child_id(event, la_code):
    """
    Creates an identifier from a combination of the Child Unique ID and Local Authority so matching child IDs
    are not removed in the merging a de-duping steps

    :param event: A filtered list of event objects
    :param la_code: The 3-character LA code used to identify a local authority
    :return: An updated list of event objects
    """
    la_child_id = f"{event.text}_{la_code}"
    yield event.from_event(event, text=la_child_id)


# @streamfilter(check=checks.type_check(events.TextNode), fail_function=pass_event)
# def get_person_school_year(event):
#     if event.text.dt.month >= 9:
#         return event.from_event()
#
#
# def get_person_school_year(data):
#     if data["PersonBirthDate"].dt.month >= 9:
#         data["PersonSchoolYear"] = data["PersonBirthDate"].dt.year
#     elif data["PersonBirthDate"].dt.month <= 8:
#         data["PersonSchoolYear"] = data["PersonBirthDate"].dt.year - 1
#     else:
#         data["PersonSchoolYear"] = None
#     return data


def populate(stream, input, la_code):
    stream = add_year(stream, input=input)

    stream = create_la_child_id(stream, la_code=la_code)
    return stream
