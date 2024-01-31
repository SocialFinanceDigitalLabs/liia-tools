import unittest

from sfdata_stream_parser.events import StartElement, EndElement, TextNode

from liiatools.common.stream_record import (
    _reduce_dict,
    text_collector,
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
