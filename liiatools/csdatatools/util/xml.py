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


def dom_parse(source, **kwargs):
    """
    Equivalent of the xml parse included in the sfdata_stream_parser package, but uses the ET DOM
    and allows direct DOM manipulation.
    """
    parser = etree.iterparse(source, events=("start", "end", "comment", "pi"), **kwargs)
    for action, elem in parser:
        if action == "start":
            yield StartElement(tag=elem.tag, attrib=elem.attrib, node=elem)
            if elem.text:
                yield TextNode(text=elem.text)
        elif action == "end":
            yield EndElement(tag=elem.tag, node=elem)
            if elem.tail:
                yield TextNode(text=elem.tail)
        elif action == "comment":
            yield CommentNode(text=elem.text, node=elem)
        elif action == "pi":
            yield ProcessingInstructionNode(name=elem.target, text=elem.text, node=elem)
        else:
            raise ValueError(f"Unknown event: {action}")


def to_xml(stream, builder: etree.TreeBuilder):
    for ev in stream:
        if isinstance(ev, StartElement):
            builder.start(ev.tag, getattr(ev, "attrs", {}))
        elif isinstance(ev, EndElement):
            builder.end(ev.tag)
        elif isinstance(ev, TextNode):
            builder.data(ev.text)
        elif isinstance(ev, CommentNode):
            builder.comment(ev.text)
        elif isinstance(ev, ProcessingInstructionNode):
            builder.pi(ev.name, ev.text)
        yield ev
