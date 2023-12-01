from typing import Iterator

import pandas as pd
import tablib
from more_itertools import peekable
from enum import Enum

from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import (generator_with_value)


class CSWWEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Worker"
    pass


class LALevelEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "LA_Level"
    pass


class HeaderEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Header"
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
    data_dict = {}
    current_element = None
    for event in stream:
        if isinstance(event, events.StartElement):
            current_element = event.tag
        if isinstance(event, events.TextNode) and event.cell:
            data_dict.setdefault(current_element, []).append(event.cell)
    return _reduce_dict(data_dict)


def message_collector(stream):
    """
    Collect messages from XML elements and yield events

    :param stream: An iterator of events from an XML parser
    :yield: Events of type HeaderEvent, CSWWEvent or LALevelEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", f"Expected Message, got {stream.peek().tag}"
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
        elif event.get("tag") == "LALevelVacancies":
            lalevel_record = text_collector(stream)
            if lalevel_record:
                yield LALevelEvent(record=lalevel_record)
        else:
            next(stream)


@generator_with_value
def export_table(stream):
    dataset = {}
    for event in stream:
        event_type = type(event)
        dataset.setdefault(event_type.name(), []).append(event.as_dict()["record"])
        yield event
    return dataset
