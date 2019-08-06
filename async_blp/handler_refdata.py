"""
Handlers create own session and have all events and queues to async work with
Bloomberg
"""

import asyncio
from typing import List

from async_blp.abs_handler import AbcHandler

try:
    import blpapi
except ImportError:
    from tests import env_test as blpapi


class HandlerRef(AbcHandler):
    """
    Handler get response event from Bloomberg from other thead and work async
    with it
    """
    service_name = "//blp/refdata"
    request_name = "ReferenceDataRequest"

    def __init__(self, start_session=True):
        """
        important startAsync before doing smt else
        """
        super().__init__()
        self.requests = {}
        self.connection = asyncio.Event()
        self.loop = asyncio.get_running_loop()
        self.complete_event: asyncio.Event = asyncio.Event()
        session_options = blpapi.SessionOptions()
        session_options.setServerHost("localhost")
        session_options.setServerPort(8194)
        self.__result = []

        self.session = blpapi.Session(options=session_options,
                                      eventHandler=self)
        if start_session:
            self.session.startAsync()

    def send_requests(self, requests: List):
        """
        save and prepare requests
        """
        self.requests['id'] = requests

    async def _send_requests(self):
        """
        Find correct moment to send requests
        """
        await self.connection.wait()
        service = self.session.getService(self.service_name)
        for _, request_obj in self.requests.items():
            request_obj.send_requests(service)
        self.complete_event.clear()

    def __call__(self, event: blpapi.Event, session: blpapi.Session):
        """
        work with response event from Bloomberg
        """
        print('got type ', event.eventType())
        for msg in event:
            self.__result.append(msg)
            if msg.asElement().name() == 'SessionStarted':
                session.openServiceAsync(self.service_name)
            if msg.asElement().name() == 'ServiceOpened':
                self.loop.call_soon_threadsafe(lambda event_: event_.set(),
                                               self.connection)
            if event.eventType() == blpapi.Event.RESPONSE:
                self.loop.call_soon_threadsafe(lambda event_: event_.set(),
                                               self.complete_event)
