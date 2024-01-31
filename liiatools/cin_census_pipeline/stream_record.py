from more_itertools import peekable
from typing import Iterator

from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import generator_with_value

from liiatools.common.stream_record import text_collector, HeaderEvent, _reduce_dict


class CINEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "CIN"

    pass


@xml_collector
def cin_collector(stream):
    """
    Create a dictionary of text values for each CIN element;
    Assessments, CINPlanDates, Section47 and ChildProtectionPlans

    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        if event.get("tag") in (
            "Assessments",
            "CINPlanDates",
            "Section47",
            "ChildProtectionPlans",
        ):
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
        else:
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def child_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails

    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    assert stream.peek().tag == "Child"
    while stream:
        event = stream.peek()
        if event.get("tag") in ("ChildIdentifiers", "ChildCharacteristics"):
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
        elif event.get("tag") == "CINdetails":
            data_dict.setdefault(event.tag, []).append(cin_collector(stream))
        else:
            next(stream)

    return _reduce_dict(data_dict)


def message_collector(stream):
    """
    Collect messages from XML elements and yield events

    :param stream: An iterator of events from an XML parser
    :yield: Events of type HeaderEvent or CINEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", "Expected Message, got {}".format(
        stream.peek().tag
    )
    while stream:
        event = stream.peek()
        if event.get("tag") == "Header":
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
        elif event.get("tag") == "Child":
            cin_record = child_collector(stream)
            if cin_record:
                yield CINEvent(record=cin_record)
        else:
            next(stream)


__EXPORT_HEADERS = [
    "LAchildID",
    "Date",
    "Type",
    "CINreferralDate",
    "ReferralSource",
    "PrimaryNeedCode",
    "CINclosureDate",
    "ReasonForClosure",
    "DateOfInitialCPC",
    "ReferralNFA",
    "CINPlanStartDate",
    "CINPlanEndDate",
    "S47ActualStartDate",
    "InitialCPCtarget",
    "ICPCnotRequired",
    "AssessmentActualStartDate",
    "AssessmentInternalReviewDate",
    "AssessmentAuthorisationDate",
    "Factors",
    "CPPstartDate",
    "CPPendDate",
    "InitialCategoryOfAbuse",
    "LatestCategoryOfAbuse",
    "NumberOfPreviousCPP",
    "UPN",
    "FormerUPN",
    "UPNunknown",
    "PersonBirthDate",
    "ExpectedPersonBirthDate",
    "GenderCurrent",
    "PersonDeathDate",
    "Ethnicity",
    "Disabilities",
]


def _maybe_list(value):
    """
    Ensures that the given value is a list.

    Parameters:
    - value: The value to be converted to a list. It can be of any type.

    Returns:
    - A list containing the original value(s).

    Behavior:
    - If the input value is None, the function returns an empty list.
    - If the input value is already a list, it is returned as is.
    - For any other value, the function wraps it in a list and returns it.
    """
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def cin_event(record, property, event_name=None, export_headers=__EXPORT_HEADERS):
    """
    Create an event record based on the given property from the original record.

    This function takes a dictionary `record` and extracts the value of a specified
    `property`. If the property exists and is non-empty, it creates a new dictionary
    with keys "Date" and "Type" where "Date" is the value of the specified property
    and "Type" is the name of the event. The new dictionary is then filtered based
    on the keys specified in export_headers.

    Parameters:
    - record (dict): The original record containing various key-value pairs.
    - property (str): The key in the `record` dictionary to look for.
    - event_name (str, optional): The name of the event. Defaults to the value of `property` if not specified.
    - export_headers (list, optional): A list of keys to include in the returned dictionary.
    Defaults to `__EXPORT_HEADERS`.

    Returns:
    - tuple: A single-element tuple containing the new filtered dictionary, or an empty tuple if `property` is not
    found or its value is empty.

    Note:
    - The reason this returns a tuple is that when called, it is used with 'yield from' which expects an iterable.
    An empty tuple results in no records being yielded.
    """
    if event_name is None:
        event_name = property
    value = record.get(property)
    if value:
        new_record = {**record, "Date": value, "Type": event_name}
        return {k: new_record.get(k) for k in export_headers},

    return ()


def event_to_records(event: CINEvent) -> Iterator[dict]:
    """
    Transforms a CINEvent into a series of event records.

    The CINEvent has to have a record. This is generated from :func:`child_collector`

    Parameters:
    - event (CINEvent): A CINEvent object containing the record and various details related to it.

    Returns:
    - Iterator[dict]: An iterator that yields dictionaries representing individual event records.

    Behavior:
    - The function first creates a 'child' dictionary by merging the "ChildIdentifiers" and "ChildCharacteristics"
    from the original event record.
    - It then processes various sub-records within the event, including "CINdetails", "Assessments", "CINPlanDates",
    "Section47", and "ChildProtectionPlans".
    - Each sub-record is further processed and emitted as an individual event record.
    """
    record = event.record
    child = {
        **record.get("ChildIdentifiers", {}),
        **record.get("ChildCharacteristics", {}),
    }
    child["Disabilities"] = ",".join(_maybe_list(child.get("Disability")))

    for cin_item in _maybe_list(record.get("CINdetails")):
        yield from cin_event({**child, **cin_item}, "CINreferralDate")
        yield from cin_event({**child, **cin_item}, "CINclosureDate")

        for assessment in _maybe_list(cin_item.get("Assessments")):
            assessment["Factors"] = ",".join(
                _maybe_list(assessment.get("AssessmentFactors"))
            )
            yield from cin_event(
                {**child, **cin_item, **assessment}, "AssessmentActualStartDate"
            )
            yield from cin_event(
                {**child, **cin_item, **assessment}, "AssessmentAuthorisationDate"
            )

        for cin in _maybe_list(cin_item.get("CINPlanDates")):
            yield from cin_event(
                {**child, **cin_item, **cin},
                "CINPlanStartDate",
            )
            yield from cin_event({**child, **cin_item, **cin}, "CINPlanEndDate")

        for s47 in _maybe_list(cin_item.get("Section47")):
            yield from cin_event({**child, **cin_item, **s47}, "S47ActualStartDate")

        for cpp in _maybe_list(cin_item.get("ChildProtectionPlans")):
            yield from cin_event({**child, **cin_item, **cpp}, "CPPstartDate")
            yield from cin_event({**child, **cin_item, **cpp}, "CPPendDate")
            for cpp_review in _maybe_list(cpp.get("CPPreviewDate")):
                cpp_review = {"CPPreviewDate": cpp_review}
                yield from cin_event(
                    {**child, **cin_item, **cpp, **cpp_review}, "CPPreviewDate"
                )


@generator_with_value
def export_table(stream):
    """
    Collects all the records into a dictionary of lists of rows

    This filter requires that the stream has been processed by `message_collector` first

    :param stream: An iterator of events from message_collector
    :yield: All events
    :return: A dictionary of lists of rows, keyed by record name
    """
    dataset = {}
    for event in stream:
        event_type = type(event)
        for record in event_to_records(event):
            dataset.setdefault(event_type.name(), []).append(record)
        yield event
    return dataset
