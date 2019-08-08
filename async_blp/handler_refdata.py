"""
File contains handler for ReferenceDataRequest
"""

import asyncio
import uuid
from typing import Dict
from typing import List

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
        await self.services[name].wait()
        service = self.session.getService(name)
        return service

    def __call__(self, event: blpapi.Event, session: blpapi.Session):
        """
        Process response event from Bloomberg
        """
        type_ = event.eventType()
        for msg in event:
            correlation_ids = [cor_id for cor_id in msg.correlationIds()]
            if msg.asElement().name() == 'SessionStarted':
                # you need wait this event before open session
                self.loop.call_soon_threadsafe(lambda event_: event_.set(),
                                               self.connection)

            if msg.asElement().name() == 'ServiceOpened':
                # you need wait this event before SEND request
                for opne_event in self.services.values():
                    self.loop.call_soon_threadsafe(lambda event_: event_.set(),
                                                   opne_event)

            if type_ == blpapi.Event.RESPONSE:
                # last event
                for corr_id in correlation_ids:
                    request = self.requests[corr_id]
                    self.loop.call_soon_threadsafe(
                        request.msg_queue.put_nowait,
                        msg)

                    self.loop.call_soon_threadsafe(
                        request.msg_queue.put_nowait,
                        blpapi.Event.RESPONSE)
