from more_itertools import peekable

from sfdata_stream_parser import events

from liiatools.common.stream_record import text_collector, HeaderEvent


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
