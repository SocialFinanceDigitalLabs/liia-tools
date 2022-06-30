from csdatatools.datasets.cincensus.schema import Schema

from liiatools.datasets.cin_census.lds_cin_clean.cleaner import (
    validate_elements,
)

from sfdata_stream_parser import events


def test_validate_elements():
    path = r"Message/Children/Child/CINdetails/ChildProtectionPlans"
    tag = "ChildProtectionPlans"
    schema = Schema().schema.get_element(tag, path)

    stream = validate_elements(
        [
            events.StartElement(schema=schema,
                                # node=<Element Reviews at 0x2beaea955c0>
                                )
        ]
    )
