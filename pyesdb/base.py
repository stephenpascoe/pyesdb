from typing import Optional, Union, Iterator, Iterable, Any, Dict
import dataclasses
from dataclasses import dataclass
from abc import ABC, abstractmethod


class Event:
    """Inherit from this class to create event types.  Events should be dataclasses."""
    __subclass_cache = {}

    @classmethod
    def get_event_class(cls, event_type: str) -> 'Event':
        if event_type in cls.__subclass_cache:
            return cls.__subclass_cache[event_type]
        else:
            for subclass in cls.__subclasses__():
                if subclass.__name__ == event_type:
                    cls.__subclass_cache[event_type] = subclass
                    return subclass

        raise ValueError(f'No event_type {event_type}')


@dataclass
class StreamEvent:
    data: Event
    id: str
    stream_revision: int


class AbstractEventStore(ABC):
    @abstractmethod
    def iter_stream(
        self,
        stream_name: bytes,
        count: Optional[int] = None,
        backwards: bool = False,
        revision: Optional[int] = None,
    ) -> Iterator[StreamEvent]:
        pass

    @abstractmethod
    def iter_all(
        self, count: Optional[int] = None, backwards: bool = False, position: int = 0,
        timeout: Optional[float] = None
    ) -> Iterator[Any]:
        pass

    @abstractmethod
    def send_events(
        self,
        stream_name: bytes,
        events: Iterable[Event],
        stream_exists: Optional[bool] = None,
    ) -> Any:
        pass

    @abstractmethod
    def delete_stream(self, stream_name: bytes) -> bool:
        pass
