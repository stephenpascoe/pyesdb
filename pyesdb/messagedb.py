"""
Interface to MessageDB.

"""

from typing import Dict, Any, Optional, Iterator, Iterable
import dataclasses
import json

import psycopg2
from psycopg2.extras import Json

from .base import AbstractEventStore, Event, StreamEvent


class MessageDB(AbstractEventStore):
    def __init__(self, **conn_params: Dict[str, Any]) -> None:
        db = conn_params.setdefault('database', 'message_store')
        opts = conn_params.get('options', '')
        conn_params['options'] = f'-c search_path={db},public {opts}'

        self.conn = psycopg2.connect(**conn_params)

    def iter_stream(
        self,
        stream_name: bytes,
        count: Optional[int] = None,
        backwards: bool = False,
        revision: Optional[int] = None,
    ) -> Iterator[StreamEvent]:
        if backwards:
            raise NotImplementedError('Backwards reading not supported for MessageDB')
        # TODO : Do something with NOTIFY
        if count is None:
            raise NotImplementedError('Streaming not implemented')
        with self.conn.cursor() as c:
            c.execute("""select id as uuid, type as event_, position, data
                         from get_stream_messages(%(stream_name)s, %(revision)s, %(count)s)""",
                      {'stream_name': stream_name, 'revision': revision, 'count': count})
            for uuid, event_type, position, data in c:
                event_class = Event.get_event_class(event_type)
                data = event_class(**json.loads(data))
                stream_revision = position

                yield StreamEvent(id=uuid, data=data, stream_revision=stream_revision)

    def iter_all(
        self, count: Optional[int] = None, backwards: bool = False, position: int = 0,
        timeout: Optional[float] = None
    ) -> Iterator[Any]:
        pass

    def send_events(
        self,
        stream_name: bytes,
        events: Iterable[Event],
        stream_exists: Optional[bool] = None,
    ) -> Any:
        if stream_exists is not None:
            raise ValueError('stream_exists option is not implemented')

        with self.conn.cursor() as c:
            for event in events:
                c.execute("""select write_message(
                               gen_random_uuid()::varchar, %(stream_name)s, %(event_type)s, %(data)s
                             )""",
                          {'stream_name': stream_name, 'event_type': event.__class__.__name__,
                           'data': Json(dataclasses.asdict(event))
                           })
        self.conn.commit()


    def delete_stream(self, stream_name: bytes) -> bool:
        pass
