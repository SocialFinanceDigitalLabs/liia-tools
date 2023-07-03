from typing import Iterator
import tablib
from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector


class CINEvent(events.ParseEvent):
    pass


class HeaderEvent(events.ParseEvent):
    pass


def _reduce_dict(dict_instance):
    new_dict = {}
    for key, value in dict_instance.items():
        if len(value) == 1:
            new_dict[key] = value[0]
        else:
            new_dict[key] = value
    return new_dict


@xml_collector
def text_collector(stream):
    data_dict = {}
    current_element = None
    for event in stream:
        if isinstance(event, events.StartElement):
            current_element = event.tag
        if isinstance(event, events.TextNode) and event.text:
            data_dict.setdefault(current_element, []).append(event.text)

    return _reduce_dict(data_dict)


@xml_collector
def cin_collector(stream):
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
            if isinstance(event, events.TextNode) and event.text:
                data_dict.setdefault(last_tag, []).append(event.text)
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def child_collector(stream):
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


@xml_collector
def message_collector(stream):
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
    "PersonSchoolYear",
    "Ethnicity",
    "Disabilities",
]


def _maybe_list(value):
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def cin_event(record, property, event_name=None):
    if event_name is None:
        event_name = property
    value = record.get(property)
    if value:
        new_record = {**record, "Date": value, "Type": event_name}
        return ({k: new_record.get(k) for k in __EXPORT_HEADERS},)

    return ()


def event_to_records(event: CINEvent) -> Iterator[dict]:
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


def export_table(stream):
    data = tablib.Dataset(headers=__EXPORT_HEADERS)
    for event in stream:
        if isinstance(event, CINEvent):
            for record in event_to_records(event):
                data.append([record.get(k, "") for k in __EXPORT_HEADERS])
    return data
