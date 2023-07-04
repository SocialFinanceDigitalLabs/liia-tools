from typing import Iterator
import tablib
from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector


class CSWWEvent(events.ParseEvent):
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
        elif event.get("tag") == "CSWWWorker":
            csww_record = text_collector(stream)
            if csww_record:
                yield CSWWEvent(record=csww_record)
        else:
            next(stream)


__EXPORT_HEADERS = [
    "AgencyWorker",
    "SWENo",
    "FTE",
    "PersonBirthDate",
    "GenderCurrent",
    "Ethnicity",
    "QualInst",
    "StepUpGrad",
    "RoleStartDate",
    "StartOrigin",
    "Cases30",
    "WorkingDaysLost",
    "ContractWeeks",
    "FrontlineGrad",
    "Absat30Sept",
    "ReasonAbsence",
    "CFKSSstatus",
]


def _maybe_list(value):
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def event_to_records(event: CSWWEvent) -> Iterator[dict]:
    record = event.record
    for item in _maybe_list(record):
        yield from (item,)


# need to check from here:
def export_table(stream):
    data = tablib.Dataset(headers=__EXPORT_HEADERS)
    for event in stream:
        if isinstance(event, CSWWEvent):
            for record in event_to_records(event):
                data.append([record.get(k, "") for k in __EXPORT_HEADERS])
    return data
