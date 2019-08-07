"""
File contains handler for ReferenceDataRequest
"""

import asyncio
from typing import Dict
from typing import List

from async_blp.abs_handler import AbsHandler
from async_blp.requests import ReferenceDataRequest

try:
    import blpapi
except ImportError:
    from async_blp import env_test as blpapi


class HandlerRef(AbsHandler):
    """
    Handler gets response events from Bloomberg from other thread, then
    asynchronously processes it. Each handler opens its own session
    """
    service_name = "//blp/refdata"
    request_name = "ReferenceDataRequest"

    def __init__(self, start_session=True):
        """
        It is important to start session with startAsync before doing anything
        else
        """
        super().__init__()

        self.requests: Dict[str, ReferenceDataRequest] = {}
        self.connection = asyncio.Event()
        self.loop = asyncio.get_running_loop()
        self.complete_event: asyncio.Event = asyncio.Event()
        self.queue = asyncio.Queue()

        session_options = blpapi.SessionOptions()
        session_options.setServerHost("localhost")
        session_options.setServerPort(8194)

        self.session = blpapi.Session(options=session_options,
                                      eventHandler=self)
        if start_session:
            self.session.startAsync()

    def send_requests(self, requests: List[ReferenceDataRequest]):
        """
        save and prepare requests
        """
        self.requests['id'] = requests

    async def _send_requests(self):
        """
        Wait until session is started and service is opened, then send requests
        """
        await self.connection.wait()
        service = self.session.getService(self.service_name)

        for request in self.requests.values():
            blp_request = request.create(service)
            self.session.sendRequest(blp_request)

        self.complete_event.clear()

    def __call__(self, event: blpapi.Event, session: blpapi.Session):
        """
        Process response event from Bloomberg
        """
        print('got type ', event.eventType())
        for msg in event:

            if msg.asElement().name() == 'SessionStarted':
                session.openServiceAsync(self.service_name)

            if msg.asElement().name() == 'ServiceOpened':
                self.loop.call_soon_threadsafe(lambda event_: event_.set(),
                                               self.connection)

            if event.eventType() == blpapi.Event.RESPONSE:
                corr_id = '1'
                request = self.requests[corr_id]

                self.loop.call_soon_threadsafe(
                    lambda msg: request.msg_queue.put(msg),
                    msg)

                self.loop.call_soon_threadsafe(
                    lambda msg: request.msg_queue.put('END'),
                    msg)
