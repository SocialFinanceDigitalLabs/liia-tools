import tablib

from sfdata_stream_parser import events

from liiatools.datasets.annex_a.lds_annexa_clean import (
    file_creator
)


def test_save_tables():
    data = tablib.Dataset(headers=["Name", "Age"])
    data.append(("Kenneth", 12))

    stream = file_creator.save_tables(
        [
            events.StartContainer(),

            events.StartTable(sheet_name="List 1"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 2"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 3"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 4"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 5"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 6"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 7"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 8"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 9"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 10"),
            file_creator.TableEvent(data=data),
            events.StartTable(sheet_name="List 11"),
            file_creator.TableEvent(data=data),

            events.EndContainer(filename="test"),

        ],
        "Output"
    )

