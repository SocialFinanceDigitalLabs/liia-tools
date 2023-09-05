import re
from typing import List

import xmlschema
from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.collectors import block_check, collector
from sfdata_stream_parser.filters.generic import pass_event, streamfilter
from xmlschema import XMLSchemaValidatorError

_INVALID_TAGS_TO_REMOVE = [
    "LAchildID",
    "UPN",
    "FormerUPN",
    "UPNunknown",
    "PersonBirthDate",
    "GenderCurrent",
    "PersonDeathDate",
    "Ethnicity",
    "Disability",
    "CINreferralDate",
    "ReferralSource",
    "PrimaryNeedCode",
    "CINclosureDate",
    "ReasonForClosure",
    "DateOfInitialCPC",
    "AssessmentActualStartDate",
    "AssessmentInternalReviewDate",
    "AssessmentAuthorisationDate",
    "AssessmentFactors",
    "CINPlanStartDate",
    "CINPlanEndDate",
    "S47ActualStartDate",
    "InitialCPCtarget",
    "DateOfInitialCPC",
    "ICPCnotRequired",
    "ReferralNFA",
    "CPPstartDate",
    "CPPendDate",
    "InitialCategoryOfAbuse",
    "LatestCategoryOfAbuse",
    "NumberOfPreviousCPP",
    "CPPreviewDate",
]

_EXCEPTION_LINE_PATTERN = re.compile(r"(?=\(line.*?(\w+))", re.MULTILINE)
_MISSING_FIELD_PATTERN = re.compile(r"(?=\sTag.*?(\w+))")


@streamfilter(check=type_check(events.TextNode), fail_function=pass_event)
def strip_text(event):
    """
    Strips surrounding whitespaces from :class:`sfdata_stream_parser.events.TextNode`. If the event does
    not have a text property then this filter fails silently.

    If there is no content at all, then the node is not returned.
    """
    if not hasattr(event, "text"):
        return event

    if event.text is None:
        return None

    text = event.text.strip()
    if len(text) > 0:
        return event.from_event(event, text=text)
    else:
        return None


@streamfilter(default_args=lambda: {"context": []})
def add_context(event, context: List[str]):
    """
    Adds 'context' to XML structures. For each :class:`sfdata_stream_parser.events.StartElement` the tag name is
    added to a 'context' tuple, and for each :class:`sfdata_stream_parser.events.EndElement` the context is popped.

    For all other events, the context tuple is set as-is.

    Provides: context
    """
    if isinstance(event, events.StartElement):
        context.append(event.tag)
        local_context = tuple(context)
    elif isinstance(event, events.EndElement):
        local_context = tuple(context)
        context.pop()
    else:
        local_context = tuple(context)

    return event.from_event(event, context=local_context)


@streamfilter()
def add_schema(event, schema: xmlschema.XMLSchema):
    """
    Requires each event to have event.context as set by :func:`add_context`

    Based on the context (a tuple of element tags) it will set path which is the
    derived path (based on the context tags) joined by '/' and schema holding the
    corresponding schema element, if found.

    Provides: path, schema
    """
    assert (
        event.context
    ), "This filter required event.context to be set - see add_context"
    path = "/".join(event.context)
    tag = event.context[-1]
    el = schema.get_element(tag, path)
    return event.from_event(event, path=path, schema=el)


def inherit_LAchildID(stream):
    """
    Apply the LAchildID to all elements within <Child></Child>

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    child_id = child_events = None
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == "Child":
            child_events = [event]
        elif isinstance(event, events.EndElement) and event.tag == "Child":
            child_events.append(event)
            yield from [e.from_event(e, LAchildID=child_id) for e in child_events]
            child_id = child_events = None
        elif isinstance(event, events.TextNode):
            try:
                child_events.append(event)
                if event.schema.name == "LAchildID":
                    child_id = event.text
            except (
                AttributeError
            ):  # Raised in case there is no LAchildID or event is None
                pass
        elif child_events:
            child_events.append(event)
        else:
            yield event


@streamfilter(check=type_check(events.StartElement), fail_function=pass_event)
def validate_elements(event):
    """
    Validates each element, and if not valid, sets the properties:
        * valid - (always False)
        * validation_message - a descriptive validation message

    Looks for specific text related to errors:
        * LAchildID is missing
        * Any other field is missing
        * Required field is blank
        * Attribute error (I think) this is when no schema is attached to a node because it wasn't expected. So am unexpected node error

    :param event: A filtered list of event objects
    :param LAchildID_error: An empty list to save child ID errors
    :param field_error: An empty list to save field errors

    """
    if not hasattr(event, "schema"):
        return event.from_event(event, valid=False, validation_error_type="NoSchema")

    try:
        event.schema.validate(event.node)
        return event
    except XMLSchemaValidatorError as e:
        validation_error = e

    full_error_message = str(validation_error)

    match = _EXCEPTION_LINE_PATTERN.search(full_error_message)
    error_line = match.group(1) if match else None

    attribs = dict(valid=False, validation_error=validation_error)
    if error_line:
        attribs["error_line"] = error_line

    if "Unexpected" and "LAchildID" in validation_error.reason:
        attribs["error_type"] = "MissingLAchildID"
    elif " expected" in validation_error.reason:
        attribs["error_type"] = "MissingField"
        match = _MISSING_FIELD_PATTERN.search(validation_error.reason)
        missing_field = match.group(1) if match else None
        if missing_field:
            attribs["missing_field"] = missing_field
    elif "failed validating ''" in validation_error.message:
        attribs["error_type"] = "BlankField"
    else:
        attribs["error_type"] = "Other"

    return event.from_event(event, **attribs)


@streamfilter(check=lambda x: True)
def counter(event, counter_check, value_error, structural_error, blank_error):
    """
    Count the invalid simple nodes storing their name and LAchildID data

    :param event: A filtered list of event objects
    :param counter_check: A function to identify which events to check
    :param value_error: An empty list to store the invalid element information
    :param structural_error: An empty list to store the invalid structure information
    :param blank_error: An empty list to store the blank element information
    :return: The same filtered list of event objects
    """
    if counter_check(event) and len(event.node) == 0:
        if (
            getattr(event, "LAchildID", None) is not None
        ):  # In case there are errors in the <Header> node as none
            # of these will have an LAchildID assigned
            if hasattr(event, "validation_message"):
                blank_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.schema.name}"
                )
            elif hasattr(event.schema, "name"):
                value_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.schema.name}"
                )
            else:
                structural_error.append(
                    f"LAchildID: {event.LAchildID}, Node: {event.tag}"
                )
        else:
            if hasattr(event, "validation_message"):
                blank_error.append(f"LAchildID: blank, Node: {event.schema.name}")
            elif hasattr(event.schema, "name"):
                value_error.append(f"LAchildID: blank, Node: {event.schema.name}")
            else:
                structural_error.append(f"LAchildID: blank, Node: {event.tag}")
    return event


@streamfilter(check=type_check(events.TextNode), fail_function=pass_event)
def convert_true_false(event):
    """
    Search for any events that have the schema type="yesnotype" and convert any values of false to 0 and true to 1

    :param event: A filtered list of event objects
    :return: An updated list of event objects
    """
    if hasattr(event, "schema"):
        if event.schema.type.name == "yesnotype":
            if event.text.lower() == "false":
                event = event.from_event(event, text="0")
            elif event.text.lower() == "true":
                event = event.from_event(event, text="1")
    return event


@collector(check=block_check(events.StartElement), receive_stream=True)
def remove_invalid(stream):
    """
    Filters out events with the given tag name if they are not valid

    :param stream: A filtered list of event objects
    :return: An updated list of event objects
    """
    stream = list(stream)
    first = stream[0]
    last = stream[-1]
    stream = stream[1:-1]

    if first.tag in _INVALID_TAGS_TO_REMOVE and not getattr(first, "valid", True):
        yield from []
    else:
        yield first

        if len(stream) > 0:
            yield from remove_invalid(stream)

        yield last
