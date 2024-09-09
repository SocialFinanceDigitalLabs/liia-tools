from typing import Iterator
import tablib
from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector


class PersonEvent(events.ParseEvent):
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
    """
    Create a dictionary of text values for each element

    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    print("Running text_collector")
    data_dict = {}
    current_element = None
    for event in stream:
        if isinstance(event, events.StartElement):
            current_element = event.tag
            print(f"StartElement: <{event.tag}>")
        if isinstance(event, events.EndElement):
            print(f"EndElement: <{event.tag}> context: {event.context}")
        if isinstance(event, events.TextNode) and event.text:
            print(f"TextNode: {event.text}")
            data_dict.setdefault(current_element, []).append(event.text)

    return _reduce_dict(data_dict)

@xml_collector
def message_collector(stream):
    """
    Collect messages from XML elements and yield events

    :param stream: An iterator of events from an XML parser
    :yield: Events of type HeaderEvent or PersonEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", "Expected Message, got {}".format(
        stream.peek().tag
    )
    while stream:
        event = stream.peek()
        print(f'Tag: {event.get("tag")}')
        if event.get("tag") == "Header":
            print("<Header> tag identified")
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
                print("HeaderEvent yielded")
        elif event.get("tag") == "Persons":
            print("<Persons> tag identified")
            person_record = text_collector(stream)
            if person_record:
                yield PersonEvent(record=person_record)
                print("PersonEvent yielded")
        else:
            print("Nothing to yield")
            next(stream)


__EXPORT_HEADERS_PERSON = [
    "Surname",
    "Forename",
    "PersonBirthDate",
    "Sex",
    "Ethnicity",
    "Postcode",
    "UPN",
    "UniqueLearnerNumber",
    "UPNunknown",
]


def _maybe_list(value):
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def event_to_records(event) -> Iterator[dict]:
    record = event.record
    for item in _maybe_list(record):
        yield from (item,)


def export_table(stream):
    data_person = tablib.Dataset(headers=__EXPORT_HEADERS_PERSON)
    # data_lalevel = tablib.Dataset(headers=__EXPORT_HEADERS_LALEVELVAC)
    for event in stream:
        if isinstance(event, PersonEvent):
            for record in event_to_records(event):
                data_person.append(
                    [record.get(k, "") for k in __EXPORT_HEADERS_PERSON]
                )
        # elif isinstance(event, LALevelEvent):
        #     for record in event_to_records(event):
        #         data_lalevel.append(
        #             [record.get(k, "") for k in __EXPORT_HEADERS_LALEVELVAC]
        #         )
    return data_person
