from collections import Counter


def consume(stream) -> Counter:
    """
        Ensures the stream is consumed and returns a summary of the numbers of each event that has been encountered
    """
    stream_types = [type(ev) for ev in stream]
    return Counter(stream_types)


