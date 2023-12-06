from sfdata_stream_parser.events import (
    StartElement,
    EndElement,
    TextNode,
    CommentNode,
    ProcessingInstructionNode,
)

try:
    from lxml import etree
except ImportError:
    pass


def dom_parse(source, filename, **kwargs):
    """
    Equivalent of the xml parse included in the sfdata_stream_parser package, but uses the ET DOM
    and allows direct DOM manipulation.
    """
    parser = etree.iterparse(source, events=("start", "end", "comment", "pi"), **kwargs)
    for action, elem in parser:
        if action == "start":
            yield StartElement(
                tag=elem.tag, attrib=elem.attrib, node=elem, filename=filename
            )
            yield TextNode(cell=elem.text, filename=filename, text=None)
        elif action == "end":
            yield EndElement(tag=elem.tag, node=elem, filename=filename)
            if elem.tail:
                yield TextNode(cell=elem.tail, filename=filename, text=None)
        elif action == "comment":
            yield CommentNode(cell=elem.text, node=elem, filename=filename, text=None)
        elif action == "pi":
            yield ProcessingInstructionNode(
                name=elem.target,
                cell=elem.text,
                node=elem,
                filename=filename,
                text=None,
            )
        else:
            raise ValueError(f"Unknown event: {action}")
