"""
File contains handler for ReferenceDataRequest
"""

import asyncio
import uuid
from typing import Dict
from typing import List
from typing import Optional

from async_blp.abs_handler import AbsHandler
from async_blp.requests import ReferenceDataRequest

try:
    # pylint: disable=ungrouped-imports
    import blpapi
except ImportError:
    # pylint: disable=ungrouped-imports
    from async_blp import env_test as blpapi


class HandlerRef(AbsHandler):
    """
    Handler gets response events from Bloomberg from other thread, then
    asynchronously processes it. Each handler opens its own session
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions):
        """
        It is important to start session with startAsync before doing anything
        else
        """
        super().__init__()
        self.requests: Dict[blpapi.CorrelationId, ReferenceDataRequest] = {}
        self.connection = asyncio.Event()
        self.services: Dict[str, asyncio.Event()] = {}
        self.loop = asyncio.get_running_loop()
        self.session = blpapi.Session(options=session_options,
                                      eventHandler=self)

        self.session.startAsync()
        self.method_map = {
            blpapi.Event.SESSION_STATUS:   self._session_handler,
            blpapi.Event.SERVICE_STATUS:   self._service_handler,
            blpapi.Event.RESPONSE:         self._response_handler,
            blpapi.Event.PARTIAL_RESPONSE: self._partial_handler,
            }

    async def send_requests(self, requests: List[ReferenceDataRequest]):
        """
        save and prepare requests
         Wait until session is started and service is opened,
                   then send requests
        """
        for request in requests:
            id_ = blpapi.CorrelationId(uuid.uuid4())
            self.requests[id_] = request
        await self.connection.wait()
        for id_, request in self.requests.items():
            service = await self._get_service(request.service_name)
            blp_request = request.create(service)
            self.session.sendRequest(blp_request, correlationId=id_)

    async def _get_service(self, name: str):
        """
        try open service if needed and close other if needed
        connection already must be opened
        """
        if name not in self.services:
            self.session.openServiceAsync(name)
            self.services[name] = asyncio.Event()
        print('start', self.services[name])
        await self.services[name].wait()
        service = self.session.getService(name)
        print(service)
        return service

    def _close_requests(self, requests: List[ReferenceDataRequest]):
        """
        send specific msg in all queues to close it
        """
        print('_close_requests')
        for request in requests:
            self.loop.call_soon_threadsafe(
                request.msg_queue.put_nowait,
                blpapi.Event.RESPONSE)

    def _is_error_msg(self, msg: blpapi.Event) -> bool:
        """
        responseError very large error smt wrong with connections
        """
        if msg.hasElement('responseError'):
            requests = [self.requests[cor_id]
                        for cor_id in msg.correlationIds()]
            self._close_requests(requests)
            return True

    def _session_handler(self, event_: blpapi.Event):
        """
        you need wait this event before open session
        """
        msg = list(event_)[0]
        if msg.asElement().name() == 'SessionStarted':
            self.loop.call_soon_threadsafe(lambda event_async:
                                           event_async.set(),
                                           self.connection)

    def _service_handler(self, event_: blpapi.Event):
        """
        you need wait this event before SEND request
        """
        msg = list(event_)[0]
        print("try", msg.asElement().name())
        if msg.asElement().name() == 'ServiceOpened':
            print("try", msg.asElement().name())
            for open_event in self.services.values():
                print("try", open_event)
                self.loop.call_soon_threadsafe(lambda event_async:
                                               event_async.set(),
                                               open_event)

    def _partial_handler(self, event_: blpapi.Event):
        """
        data event
        """
        for msg in event_:

            if self._is_error_msg(msg):
                continue

            for request in [self.requests[cor_id]
                            for cor_id in msg.correlationIds()]:
                self.loop.call_soon_threadsafe(
                    request.msg_queue.put_nowait,
                    msg)

    def _response_handler(self, event_: blpapi.Event):
        """
        last event
        """
        self._partial_handler(event_)
        for msg in event_:
            requests = [self.requests[cor_id]
                        for cor_id in msg.correlationIds()]
            self._close_requests(requests)

    def __call__(self, event: blpapi.Event, session: Optional[blpapi.Session]):
        """
        Process response event from Bloomberg
        """
        self.method_map[event.eventType()](event)
