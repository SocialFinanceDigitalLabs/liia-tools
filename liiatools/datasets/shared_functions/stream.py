import logging
from typing import Iterable, Union

import tablib
from sfdata_stream_parser import events

logger = logging.getLogger(__name__)


def tablib_to_stream(data: tablib.Dataset):
    """
    Parse the csv and return the row number, column number, header name and cell value

    :param input: Location of file to be cleaned
    :return: List of event objects containing filename, header and cell information
    """
    yield events.StartContainer()
    yield events.StartTable(headers=data.headers)
    for r_ix, row in enumerate(data):
        yield events.StartRow()
        for c_ix, cell in enumerate(row):
            yield events.Cell(
                r_ix=r_ix,
                c_ix=c_ix,
                header=data.headers[c_ix],
                cell=cell,
            )
        yield events.EndRow()
    yield events.EndTable()
    yield events.EndContainer()


def inherit_property(stream, prop_name: Union[str, Iterable[str]], override=False):
    """
    Reads a property from StartTable and sets that property (if it exists) on every event between this event
    and the next EndTable event.

    :param event: A filtered list of event objects of type StartTable
    :param prop_name: The property name to inherit
    :return: An updated list of event objects
    """
    if isinstance(prop_name, str):
        prop_name = [prop_name]

    prop_value = None
    for event in stream:
        if isinstance(event, events.StartTable):
            prop_value = {k: getattr(event, k, None) for k in prop_name}
            prop_value = {k: v for k, v in prop_value.items() if v is not None}
        elif isinstance(event, events.EndTable):
            prop_value = None

        if prop_value:
            if override:
                event_values = prop_value
            else:
                event_values = {
                    k: v for k, v in prop_value.items() if not hasattr(event, k)
                }
            event = event.from_event(event, **event_values)

        yield event
