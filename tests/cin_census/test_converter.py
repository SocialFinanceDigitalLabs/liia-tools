# from liiatools.datasets.cin_census.lds_cin_clean import converter
# from sfdata_stream_parser import events
#
#
# def test_convert_true_false():
#     class Schema:
#         def __init__(self):
#             self.type = Name()
#
#     class Name:
#         def __init__(self):
#             self.name = "yesnotype"
#
#     stream = converter.convert_true_false(
#         [
#             events.TextNode(text="false", schema=Schema()),
#             events.TextNode(text="true", schema=Schema()),
#             events.TextNode(text="TRUE", schema=Schema()),
#         ]
#     )
#     stream = list(stream)
#     assert stream[0].text == "0"
#     assert stream[1].text == "1"
#     assert stream[2].text == "1"
#
#     class Name:
#         def __init__(self):
#             self.name = "other_type"
#
#     stream = converter.convert_true_false(
#         [
#             events.TextNode(text="false", schema=Schema()),
#             events.TextNode(text="true", schema=Schema()),
#             events.TextNode(text="true"),
#         ]
#     )
#     stream = list(stream)
#     assert stream[0].text == "false"
#     assert stream[1].text == "true"
#     assert stream[2].text == "true"
