# Import the unittest module and the code to be tested
import unittest
from sfdata_stream_parser.events import StartElement, EndElement, TextNode
from liiatools.datasets.social_work_workforce.lds_csww_clean.csww_record import (
    text_collector,
    message_collector,
    CSWWEvent,
    LALevelEvent,
    HeaderEvent,
)
from liiatools.datasets.social_work_workforce.lds_csww_clean.xml import dom_parse


class TestRecord(unittest.TestCase):
    def generate_text_element(self, tag: str, text):
        """
        Create a complete TextNode sandwiched between a StartElement and EndElement

        :param tag: XML tag
        :param text: text to be stored in the given XML tag, could be a string, integer, float etc.
        :return: StartElement and EndElement with given tags and TextNode with given text
        """
        yield StartElement(tag=tag)
        yield TextNode(text=str(text))
        yield EndElement(tag=tag)

    def generate_test_csww_file(self):
        """
        Generate a sample children's social work workforce census file

        :return: stream of generators containing information required to create an XML file
        """
        yield StartElement(tag="Message")
        yield StartElement(tag="Header")
        yield from self.generate_text_element(tag="Version", text=1)
        yield EndElement(tag="Header")
        yield StartElement(tag="LALevelVacancies")
        yield from self.generate_text_element(tag="NumberOfVacancies", text=100)
        yield EndElement(tag="LALevelVacancies")
        yield StartElement(tag="CSWWWorker")
        yield from self.generate_text_element(tag="ID", text=100)
        yield from self.generate_text_element(tag="SWENo", text="AB123456789")
        yield from self.generate_text_element(tag="Agency", text=0)
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


# Run the tests
if __name__ == "__main__":
    unittest.main()
