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
            yield StartElement(tag=elem.tag, attrib=elem.attrib, node=elem, filename=filename)
            yield TextNode(text=elem.text, filename=filename)
        elif action == "end":
            yield EndElement(tag=elem.tag, node=elem, filename=filename)
            if elem.tail:
                yield TextNode(text=elem.tail, filename=filename)
        elif action == "comment":
            yield CommentNode(text=elem.text, node=elem, filename=filename)
        elif action == "pi":
            yield ProcessingInstructionNode(name=elem.target, text=elem.text, node=elem, filename=filename)
        else:
            raise ValueError(f"Unknown event: {action}")
