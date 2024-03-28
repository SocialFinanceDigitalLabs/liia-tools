from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event


@streamfilter(check=type_check(events.TextNode), fail_function=pass_event)
def convert_true_false(event):
    """
    Search for any events that have the schema type="yesnotype" and convert any values of false to 0 and true to 1

    :param event: A filtered list of event objects
    :return: An updated list of event objects
    """
    if hasattr(event, "schema"):
        if event.schema.type.name == "yesnotype":
            if event.text.lower() == "false":
                event = event.from_event(event, text="0")
            elif event.text.lower() == "true":
                event = event.from_event(event, text="1")
    return event
