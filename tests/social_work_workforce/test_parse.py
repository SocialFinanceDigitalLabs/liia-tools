from io import BytesIO

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
                '<a><?PITarget PIContent?><b>1<c frog="f">2a<!-- yeah -->2b<d/>3</c></b>4</a>'.encode(
                    "utf-8"
                )
            ),
            filename="csww.xml",
        )
    )

    events = [(type(event), event.as_dict()) for event in stream]

    assert events[0][0] == StartElement
    assert (
        events[0][1].items()
        >= {"attrib": {}, "filename": "csww.xml", "tag": "a"}.items()
    )

    assert events[1][0] == TextNode
    assert (
        events[1][1].items()
        >= {"cell": None, "filename": "csww.xml", "text": None}.items()
    )

    assert events[2][0] == ProcessingInstructionNode
    assert (
        events[2][1].items()
        >= {
            "cell": "PIContent",
            "filename": "csww.xml",
            "name": "PITarget",
            "text": None,
        }.items()
    )

    assert events[3][0] == StartElement
    assert (
        events[3][1].items()
        >= {"attrib": {}, "filename": "csww.xml", "tag": "b"}.items()
    )

    assert events[4][0] == TextNode
    assert (
        events[4][1].items()
        >= {"cell": "1", "filename": "csww.xml", "text": None}.items()
    )

    assert events[5][0] == StartElement
    assert (
        events[5][1].items()
        >= {"attrib": {"frog": "f"}, "filename": "csww.xml", "tag": "c"}.items()
    )

    assert events[6][0] == TextNode
    assert (
        events[6][1].items()
        >= {"cell": "2a", "filename": "csww.xml", "text": None}.items()
    )

    assert events[7][0] == CommentNode
    assert (
        events[7][1].items()
        >= {"cell": " yeah ", "filename": "csww.xml", "text": None}.items()
    )

    assert events[8][0] == StartElement
    assert (
        events[8][1].items()
        >= {"attrib": {}, "filename": "csww.xml", "tag": "d"}.items()
    )

    assert events[9][0] == TextNode
    assert (
        events[9][1].items()
        >= {"cell": None, "filename": "csww.xml", "text": None}.items()
    )

    assert events[10][0] == EndElement
    assert events[10][1].items() >= {"filename": "csww.xml", "tag": "d"}.items()

    assert events[11][0] == TextNode
    assert (
        events[11][1].items()
        >= {"cell": "3", "filename": "csww.xml", "text": None}.items()
    )

    assert events[12][0] == EndElement
    assert events[12][1].items() >= {"filename": "csww.xml", "tag": "c"}.items()

    assert events[13][0] == EndElement
    assert events[13][1].items() >= {"filename": "csww.xml", "tag": "b"}.items()

    assert events[14][0] == TextNode
    assert (
        events[14][1].items()
        >= {"cell": "4", "filename": "csww.xml", "text": None}.items()
    )

    assert events[15][0] == EndElement
    assert events[15][1].items() >= {"filename": "csww.xml", "tag": "a"}.items()
