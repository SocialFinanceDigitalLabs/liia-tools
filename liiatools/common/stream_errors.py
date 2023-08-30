from sfdata_stream_parser.events import ParseEvent


class EventErrors:
    """A very basic 'error' container based on a list of dicts

    This is used to store errors in the stream pipeline.

    **WARNING:** For permformance reasons, this uses a mutable error object. This should not normally be an issue, but if you serialize the stream it may cause problems.

    To add errors to the stream, use the `add_to_event` method.

    event = ... # do something with the events
    event = EventErrors.add_to_event(event, "error_type", "error message", **kwargs)

    event will now have an `errors` property set with the error message added to it. Errors must have a type and message, but other properties can be added as appropriate.
    """

    property = "errors"

    def __init__(self):
        self.__errors = []

    def push(self, **kwargs):
        self.__errors.append(kwargs)

    def __iter__(self):
        return iter(self.__errors)

    @staticmethod
    def add_to_event(
        event: ParseEvent, type: str, message: str, **kwargs
    ) -> ParseEvent:
        error = getattr(event, EventErrors.property, None)
        if not error:
            error = EventErrors()
            event = event.from_event(event, errors=error)
        error.push(**{"type": type, "message": message, **kwargs})
        return event
