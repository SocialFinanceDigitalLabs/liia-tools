from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import generator_with_value


class HeaderEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Header"

    pass


def _reduce_dict(dict_instance):
    """
    Reduce lists in dictionary values to a single value if there is only one value in the list,
    otherwise return the list

    :param dict_instance: Dictionary containing lists in the values
    :return: Dictionary with single values in the dictionary values if list length is one
    """
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
        dataset.setdefault(event_type.name(), []).append(event.as_dict()["record"])
        yield event
    return dataset
