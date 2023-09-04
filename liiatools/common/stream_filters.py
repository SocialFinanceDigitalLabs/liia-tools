import logging
from io import BytesIO, StringIO
from typing import Iterable, Union

import tablib
from sfdata_stream_parser import events
from tablib import import_book, import_set
from tablib.core import detect_format

from liiatools.common.data import FileLocator
from liiatools.common.stream_errors import StreamError

logger = logging.getLogger(__name__)


def tablib_parse(source: FileLocator):
    """
    Parse any of the tabular formats supported by TabLib
    """
    filename = source.name
    with source.open("rb") as f:
        data = f.read()

    try:
        data = data.decode("utf-8")
        data = StringIO(data)
    except UnicodeDecodeError:
        data = BytesIO(data)

    try:
        databook = import_book(data)
        return tablib_to_stream(databook, filename=filename)
    except Exception as e:
        pass

    try:
        dataset = import_set(data)
        return tablib_to_stream(dataset, filename=filename)
    except Exception as e:
        pass

    raise StreamError(f"Could not parse {source} as a tabular format")


def _tablib_dataset_to_stream(dataset: tablib.Dataset, **kwargs):
    params = {k: v for k, v in kwargs.items() if v is not None}
    yield events.StartContainer(**params)
    yield events.StartTable(headers=dataset.headers)
    for r_ix, row in enumerate(dataset):
        yield events.StartRow()
        for c_ix, cell in enumerate(row):
            yield events.Cell(
                r_ix=r_ix,
                c_ix=c_ix,
                header=dataset.headers[c_ix],
                cell=cell,
            )
        yield events.EndRow()
    yield events.EndTable()
    yield events.EndContainer()


def tablib_to_stream(
    data: Union[tablib.Dataset, tablib.Databook], filename: str = None
):
    """
    Parse the csv and return the row number, column number, header name and cell value

    :param input: Location of file to be cleaned
    :return: List of event objects containing filename, header and cell information
    """
    if isinstance(data, tablib.Dataset):
        return _tablib_dataset_to_stream(data, filename=filename)

    for sheet in data.sheets():
        return _tablib_dataset_to_stream(
            sheet, filename=filename, sheetname=sheet.title
        )


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
