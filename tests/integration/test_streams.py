"""
Test writing and reading from a stream

"""


import pytest

# Import fixtures
from . import es_client, SomeEvent


def test_write_stream(es_client):
    es_client.delete_stream(b'integration-testing')

    events = [SomeEvent(f'A test event with {x}', x) for x in range(20)]

    resp = es_client.send_events(b'integration-testing', events)
    print(resp)

    assert resp.WhichOneof('result') == 'success'


def test_write_read_stream(es_client):
    es_client.delete_stream(b'integration-testing')

    events = [SomeEvent(f'A test event with {x}', x) for x in range(20)]

    resp = es_client.send_events(b'integration-testing', events)
    assert resp.WhichOneof('result') == 'success'

    events_it = es_client.iter_stream(b'integration-testing', count=100)
    new_events = list(events_it)

    assert len(new_events) == len(events)
