"""
Client for EventStore Grpc interface

"""

from typing import Optional, Union, Iterator, Any
from enum import Enum

import grpc

from .pb.streams_pb2_grpc import StreamsStub
from .pb.streams_pb2 import ReadReq, ReadResp


class RevisionOptionEnum(Enum):
    start = 0
    end = 1



class EventStore:
    def __init__(self, connection_str: str, secure: bool = False) -> None:
        if secure:
            raise NotImplementedError("Secure Grpc is not implemented")

        self.channel = grpc.insecure_channel(connection_str)

    def streams_stub(self) -> StreamsStub:
        return StreamsStub(self.channel)

    def iter_stream(self, stream_name: bytes, count: Optional[int] = None,
                    backwards: bool = False,
                    revision: Optional[int] = None
    ) -> Iterator[Any]:

        options = _prepare_stream_options(
            stream_name=stream_name,
            backwards=backwards,
            revision=revision
        )
        req = _prepare_read_req(options=options, count=count, backwards=backwards)

        stub = self.streams_stub()
        for resp in stub.Read(req):
            # TODO : Unpack response
            yield resp


    def iter_all(self, count: Optional[int] = None,
                 backwards: bool = False,
                 position: int = 0) -> Iterator[Any]:
        options = _prepare_all_options(position=position, backwards=backwards)
        req = _prepare_read_req(options=options, count=count, backwards=backwards)

        stub = self.streams_stub()
        for resp in stub.Read(req):
            # TODO : unpack
            yield resp


def _prepare_read_req(options: ReadReq.Options,
                      count: Optional[int] = None, backwards: bool = False) -> ReadReq:
    """
    Create basic streams request without stream_options section
    """
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


def _prepare_stream_options(stream_name: str, revision: Optional[int] = None,
                            backwards: bool = False) -> ReadReq.Options:
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


def _prepare_all_options(position: Optional[int] = None, backwards: bool = False):
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
