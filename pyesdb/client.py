"""
Client for EventStore Grpc interface

"""

from typing import Optional, Union, Iterator, Iterable, Any, Dict
import uuid as uuid_m
import json as json_m
import itertools
import dataclasses
from dataclasses import dataclass
import json
from abc import ABC, abstractmethod

import grpc

from .base import AbstractEventStore, Event, StreamEvent

from .pb.streams_pb2_grpc import StreamsStub
from .pb.streams_pb2 import ReadReq, ReadResp, AppendReq, AppendResp, DeleteReq


class EventStore(AbstractEventStore):
    def __init__(self, connection_str: str, secure: bool = False) -> None:
        if secure:
            raise NotImplementedError("Secure Grpc is not implemented")

        self.channel = grpc.insecure_channel(connection_str)

    def _streams_stub(self) -> StreamsStub:
        return StreamsStub(self.channel)

    def iter_stream(
        self,
        stream_name: bytes,
        count: Optional[int] = None,
        backwards: bool = False,
        revision: Optional[int] = None,
    ) -> Iterator[StreamEvent]:

        options = _prepare_stream_options(
            stream_name=stream_name, backwards=backwards, revision=revision
        )
        req = _prepare_read_req(options=options, count=count, backwards=backwards)

        stub = self._streams_stub()
        for resp in stub.Read(req):
            # TODO : Handle other response types
            yield _resp_to_stream_event(resp)

    def iter_all(
        self, count: Optional[int] = None, backwards: bool = False, position: int = 0,
        timeout: Optional[float] = None
    ) -> Iterator[Any]:
        options = _prepare_all_options(position=position, backwards=backwards)
        req = _prepare_read_req(options=options, count=count, backwards=backwards)

        stub = self._streams_stub()
        for resp in stub.Read(req, timeout=timeout):
            # All will contain many events we don't recognise.
            # Ignore system events
            if resp.event.event.metadata['type'].startswith('$'):
                pass
            # Catch unrecognised events
            try:
                yield _resp_to_stream_event(resp)
            except ValueError:
                # TODO : Custom exception and warn
                pass

    def send_events(
        self,
        stream_name: bytes,
        events: Iterable[Event],
        stream_exists: Optional[bool] = None,
    ) -> Any:

        # TODO : Better to generate uuid in Event class
        def mkmsg(event: Event) -> AppendReq:
            req = _prepare_append_req(
                uuid=None,
                event_type=event.__class__.__name__.encode(),
                json=dataclasses.asdict(event),
            )
            return req

        # TODO : Support optimistic concurrency control
        select_req = _prepare_stream_select_req(
            stream_name=stream_name, stream_exists=stream_exists
        )
        msg_reqs = (mkmsg(event) for event in events)
        reqs = itertools.chain([select_req], msg_reqs)

        stub = self._streams_stub()
        resp = stub.Append(reqs)

        return resp

    def delete_stream(self, stream_name: bytes) -> bool:
        req = _prepare_delete_req(stream_name)

        stub = self._streams_stub()
        resp = stub.Delete(req)

        return resp.WhichOneof('position_option') == 'position'


def _prepare_read_req(
    options: ReadReq.Options, count: Optional[int] = None, backwards: bool = False
) -> ReadReq:
    req = ReadReq(options=options)
    req.options.resolve_links = True

    if count is None:
        req.options.subscription.SetInParent()
    else:
        req.options.count = count

    # TODO : filters
    req.options.no_filter.SetInParent()

    req.options.uuid_option.string.SetInParent()

    if backwards:
        req.options.read_direction = req.Options.ReadDirection.Backwards
    else:
        req.options.read_direction = req.Options.ReadDirection.Forwards

    return req


def _prepare_stream_options(
    stream_name: bytes, revision: Optional[int] = None, backwards: bool = False
) -> ReadReq.Options:
    options = ReadReq.Options()
    options.stream.stream_identifier.stream_name = stream_name

    # If revision is not specified we decide whether to read from the start of end of the stream
    # depending on the direction
    if revision is None:
        if backwards:
            options.stream.end.SetInParent()
        else:
            options.stream.start.SetInParent()
    else:
        options.stream.revision = revision

    return options


def _prepare_all_options(
    position: Optional[int] = None, backwards: bool = False
) -> ReadReq.Options:
    # TODO : support prepare_position

    options = ReadReq.Options()

    if position is None:
        if backwards:
            options.all.end.SetInParent()
        else:
            options.all.end.SetInParent()
    else:
        options.all.position.commit_position = position

    return options


def _prepare_stream_select_req(
    stream_name: bytes,
    expected_revision: Optional[int] = None,
    stream_exists: Optional[bool] = None,
) -> AppendReq:
    req = AppendReq()
    req.options.stream_identifier.stream_name = stream_name

    if expected_revision is None:
        if stream_exists is None:
            req.options.any.SetInParent()
        elif stream_exists:
            req.options.stream_exists.SetInParent()
        else:
            req.options.no_stream.SetInParent()
    else:
        if stream_exists is not None:
            raise ValueError(
                "Cannot set stream_exists when specifying expected_revision"
            )
        req.options.revision = expected_revision

    return req


def _prepare_append_req(
    uuid: Optional[uuid_m.UUID],
    event_type: bytes,
    json: Dict[str, Any],
    metadata: Optional[Dict[str, str]] = None,
    custom_metadata: Optional[bytes] = None,
) -> AppendReq:
    req = AppendReq()

    # TODO : switch to structured uuids
    if uuid is None:
        uuid = uuid_m.uuid4()

    req.proposed_message.id.string = str(uuid)

    if metadata:
        for key, value in metadata.items():
            req.proposed_message.metadata[key] = value

    req.proposed_message.metadata["type"] = event_type
    req.proposed_message.metadata["content-type"] = "application/json"

    if custom_metadata:
        req.proposed_message.custom_metadata = custom_metadata

    req.proposed_message.data = json_m.dumps(json).encode()

    return req


def _prepare_delete_req(stream_name: bytes, revision: Optional[int] = None):
    req = DeleteReq()
    req.options.stream_identifier.stream_name = stream_name
    if revision is None:
        req.options.any.SetInParent()
    else:
        req.options.revision = revision

    return req


def _resp_to_stream_event(resp: ReadResp):
    event_type = resp.event.event.metadata['type']
    event_class = Event.get_event_class(event_type)
    data = event_class(**json.loads(resp.event.event.data))
    uuid = resp.event.event.id
    stream_revision = resp.event.event.stream_revision

    return StreamEvent(id=uuid, data=data, stream_revision=stream_revision)
