import tablib
import logging
from pathlib import Path
from datetime import datetime

from sfdata_stream_parser import events

log = logging.getLogger(__name__)


def parse_csv(input):
    """
    Parse the csv and return the row number, column number, header name and cell value

    :param input: Location of file to be cleaned
    :return: List of event objects containing filename, header and cell information
    """
    filename = str(Path(input).resolve().stem)

    with open(input, "rt") as f:
        yield events.StartContainer(filename=filename)
        data = tablib.import_set(f, format="csv")
        yield events.StartTable(filename=filename, headers=data.headers)
        for r_ix, row in enumerate(data):
            yield events.StartRow(filename=filename)
            for c_ix, cell in enumerate(row):
                yield events.Cell(
                    filename=filename,
                    r_ix=r_ix,
                    c_ix=c_ix,
                    header=data.headers[c_ix],
                    cell=cell,
                )
            yield events.EndRow(filename=filename)
        yield events.EndTable(filename=filename)
        yield events.EndContainer()
