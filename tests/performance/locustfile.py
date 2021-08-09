"""
Performance testing with locust

"""

# make sure you use grpc version 1.39.0 or later,
# because of https://github.com/grpc/grpc/issues/15880 that affected earlier versions
import grpc
from locust import events, User, task
from locust.exception import LocustError
from locust.user.task import LOCUST_STATE_STOPPING
import gevent
import time
import random
from dataclasses import dataclass

import logging

from pyesdb.client import EventStore, Event

# patch grpc so that it uses gevent instead of asyncio
import grpc.experimental.gevent as grpc_gevent

grpc_gevent.init_gevent()

STREAMS = [f'perftest-{n}'.encode() for n in range(20)]

@dataclass
class SomeEvent(Event):
    message: str
    value: int


@events.init.add_listener
def initialise(environment, **_kwargs):
    client = EventStore('localhost:2113')
    for stream in STREAMS:
        client.delete_stream(stream)
    # TODO : Scavange?


class EventStoreClient:
    def __init__(self):
        self._client = EventStore('localhost:2113')

    def __getattr__(self, name):
        func = self._client.__class__.__getattribute__(self._client, name)

        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            request_meta = {
                "request_type": "grpc",
                "name": name,
                "response_length": 0,
                "exception": None,
                "context": None,
                "response": None,
            }
            try:
                request_meta["response"] = func(*args, **kwargs)
            except grpc.RpcError as e:
                request_meta["exception"] = e
            request_meta["response_time"] = (time.perf_counter() - start_time) * 1000
            events.request.fire(**request_meta)
            return request_meta["response"]

        return wrapper


class EventStoreUser(User):
    """
    Streams all messages for a while then stops
    """
    def __init__(self, environment):
        super().__init__(environment)

        self.client = EventStoreClient()

    @task
    def watchAll(self):
        logging.info('Starging read all')
        n = 0
        try:
            for event in self.client.iter_all(timeout=30.0):
                n += 1
                if n % 100 == 0:
                    print('Reading %s from all', n)
        except grpc._channel._MultiThreadedRendezvous as e:
            logging.info('Reading finished')

        logging.info('Found %s events in all', n)

        time.sleep(5)

    @task(10)
    def publish(self):
        # Choose a stream
        stream = random.choice(STREAMS)
        events = [SomeEvent(f'Performance event {x}', random.randint(0, 99)) for x in range(50)]
        self.client.send_events(stream, events)

        self.wait()
