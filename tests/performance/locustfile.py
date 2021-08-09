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

import logging

from pyesdb.client import EventStore

# patch grpc so that it uses gevent instead of asyncio
import grpc.experimental.gevent as grpc_gevent

grpc_gevent.init_gevent()


@events.init.add_listener
def initialise(environment, **_kwargs):
    # TODO : reset EventStore
    pass

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


class ReadAllUser(User):
    def __init__(self, environment):
        super().__init__(environment)

        self.client = EventStoreClient()

    @task
    def sayHello(self):
        batch = self.client.iter_all(count=10000)
        n = len(list(batch))
        logging.info('ReadAll user found %s events', n)

        time.sleep(1)
