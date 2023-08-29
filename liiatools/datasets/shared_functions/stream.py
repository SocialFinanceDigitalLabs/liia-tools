import logging

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
