from io import StringIO, BytesIO

from liiatools.csww_pipeline.stream_parse import dom_parse
from sfdata_stream_parser.events import (
    StartElement,
    EndElement,
    TextNode,
    CommentNode,
    ProcessingInstructionNode,
)


def test_dom_parse():
    stream = list(
        dom_parse(
            BytesIO(
                '<a><?PITarget PIContent?><b>1<c frog="f">2a<!-- yeah -->2b<d/>3</c></b>4</a>'.encode("utf-8")
            ),
            filename="csww.xml",
        )
    )

    events = [(type(event), event.as_dict()) for event in stream]

    assert events == [
        (StartElement, {"attrib": {}, "filename": "csww.xml", "tag": "a"}),
        (TextNode, {"cell": None, "filename": "csww.xml", "text": None}),
        (ProcessingInstructionNode, {"cell": "PIContent", "filename": "csww.xml", "name": "PITarget", "text": None}),
        (StartElement, {"attrib": {}, "filename": "csww.xml", "tag": "b"}),
        (TextNode, {"cell": "1", "filename": "csww.xml", "text": None}),
        (StartElement, {"attrib": {"frog": "f"}, "filename": "csww.xml", "tag": "c"}),
        (TextNode, {"cell": "2a", "filename": "csww.xml", "text": None}),
        (CommentNode, {"cell": " yeah ", "filename": "csww.xml", "text": None}),
        (StartElement, {"attrib": {}, "filename": "csww.xml", "tag": "d"}),
        (TextNode, {"cell": None, "filename": "csww.xml", "text": None}),
        (EndElement, {"filename": "csww.xml", "tag": "d"}),
        (TextNode, {"cell": "3", "filename": "csww.xml", "text": None}),
        (EndElement, {"filename": "csww.xml", "tag": "c"}),
        (EndElement, {"filename": "csww.xml", "tag": "b"}),
        (TextNode, {"cell": "4", "filename": "csww.xml", "text": None}),
        (EndElement, {"filename": "csww.xml", "tag": "a"}),
    ]
