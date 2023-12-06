import unittest

from sfdata_stream_parser.events import StartElement, EndElement, TextNode

from liiatools.csww_pipeline.stream_record import (
    CSWWEvent,
    LALevelEvent,
    HeaderEvent,
    _reduce_dict,
    text_collector,
    message_collector,
    export_table,
)


def test_reduce_dict():
    sample_dict = {
        "ID": ["100"],
        "SWENo": ["AB123456789"],
        "Agency": ["0"],
        "ReasonAbsence": ["MAT", "OTH"],
    }
    assert _reduce_dict(sample_dict) == {
        "ID": "100",
        "SWENo": "AB123456789",
        "Agency": "0",
        "ReasonAbsence": ["MAT", "OTH"],
    }


class TestRecord(unittest.TestCase):
    def generate_text_element(self, tag: str, cell):
        """
        Create a complete TextNode sandwiched between a StartElement and EndElement

        :param tag: XML tag
        :param cell: text to be stored in the given XML tag, could be a string, integer, float etc.
        :return: StartElement and EndElement with given tags and TextNode with given text
        """
        yield StartElement(tag=tag)
        yield TextNode(cell=str(cell), text=None)
        yield EndElement(tag=tag)

    def generate_test_csww_file(self):
        """
        Generate a sample children's social work workforce census file

        :return: stream of generators containing information required to create an XML file
        """
        yield StartElement(tag="Message")
        yield StartElement(tag="Header")
        yield from self.generate_text_element(tag="Version", cell=1)
        yield EndElement(tag="Header")
        yield StartElement(tag="LALevelVacancies")
        yield from self.generate_text_element(tag="NumberOfVacancies", cell=100)
        yield EndElement(tag="LALevelVacancies")
        yield StartElement(tag="CSWWWorker")
        yield from self.generate_text_element(tag="ID", cell=100)
        yield from self.generate_text_element(tag="SWENo", cell="AB123456789")
        yield from self.generate_text_element(tag="Agency", cell=0)
        yield EndElement(tag="CSWWWorker")
        yield EndElement(tag="Message")

    def test_text_collector(self):
        # test that the text_collector returns a dictionary of events and their text values from the stream
        test_stream = self.generate_test_csww_file()
        test_record = text_collector(test_stream)
        self.assertEqual(len(test_record), 5)
        self.assertEqual(
            test_record,
            {
                "Version": "1",
                "NumberOfVacancies": "100",
                "ID": "100",
                "SWENo": "AB123456789",
                "Agency": "0",
            },
        )

    def test_message_collector(self):
        # test that the message_collector yields events of the correct type from the stream
        test_stream = self.generate_test_csww_file()
        test_events = list(message_collector(test_stream))
        self.assertEqual(len(test_events), 3)
        self.assertIsInstance(test_events[0], HeaderEvent)
        self.assertEqual(test_events[0].record, {"Version": "1"})
        self.assertIsInstance(test_events[1], LALevelEvent)
        self.assertEqual(test_events[1].record, {"NumberOfVacancies": "100"})
        self.assertIsInstance(test_events[2], CSWWEvent)
        self.assertEqual(
            test_events[2].record, {"ID": "100", "SWENo": "AB123456789", "Agency": "0"}
        )

    def test_export_table(self):
        test_stream = self.generate_test_csww_file()
        test_events = list(message_collector(test_stream))
        dataset_holder, stream = export_table(test_events)

        self.assertEqual(len(list(stream)), 3)

        data = dataset_holder.value
        self.assertEqual(len(data), 3)
        self.assertEqual(data["Header"], [{"Version": "1"}])
        self.assertEqual(data["LA_Level"], [{"NumberOfVacancies": "100"}])
        self.assertEqual(
            data["Worker"], [{"ID": "100", "SWENo": "AB123456789", "Agency": "0"}]
        )
