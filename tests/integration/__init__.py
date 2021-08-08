"""
Integration tests assume existence of EventStore on localhost:2113 in insecure mode.

"""

import pytest
from dataclasses import dataclass

from pyesdb.client import EventStore, Event


@dataclass
class SomeEvent(Event):
    message: str
    value: int


def mk_client():
    client = EventStore("localhost:2113")

    return client


@pytest.fixture
def es_client():
    return mk_client()
