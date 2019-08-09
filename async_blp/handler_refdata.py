"""
File contains handler for ReferenceDataRequest
"""

import asyncio
import uuid
from typing import Dict
from typing import List
from typing import Optional

from async_blp.abs_handler import AbsHandler
from async_blp.log import LOGGER
from async_blp.requests import ReferenceDataRequest

try:
    # pylint: disable=ungrouped-imports
    import blpapi
except ImportError:
    # pylint: disable=ungrouped-imports
    from async_blp import env_test as blpapi

RESPONSE_ERROR = blpapi.Name('responseError')


class HandlerRef(AbsHandler):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions):

        super().__init__()
        self.requests: Dict[blpapi.CorrelationId, ReferenceDataRequest] = {}
        self.connection = asyncio.Event()
        self.services: Dict[str, asyncio.Event] = {}
        self.session = blpapi.Session(options=session_options,
                                      eventHandler=self)

        # It is important to start session with startAsync before doing anything
        # else
        self.session.startAsync()
        LOGGER.debug('%s: session started', self.__class__.__name__)

        # loop is used for internal coordination
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError('Please create handler inside asyncio loop')

        # each event type is processed by its own method
        self.method_map = {
            blpapi.Event.SESSION_STATUS:   self._session_handler,
            blpapi.Event.SERVICE_STATUS:   self._service_handler,
            blpapi.Event.RESPONSE:         self._response_handler,
            blpapi.Event.PARTIAL_RESPONSE: self._partial_handler,
            }

    async def send_requests(self, requests: List[ReferenceDataRequest]):
        """
        Send requests to Bloomberg

        Wait until session is started and required service is opened,
        then send requests
        """
        # wait until the session is opened
        await self.connection.wait()

        for request in requests:
            corr_id = blpapi.CorrelationId(uuid.uuid4())
            self.requests[corr_id] = request

            # wait until the necessary service is opened
            service = await self._get_service(request.service_name)

            blp_request = request.create(service)
            self.session.sendRequest(blp_request, correlationId=corr_id)
            LOGGER.debug('%s: request send', self.__class__.__name__)

    async def _get_service(self, service_name: str) -> blpapi.Service:
        """
        Try to open service if it wasn't opened yet. Session must be opened
        before calling this method
        """
        if service_name not in self.services:
            self.services[service_name] = asyncio.Event()
            self.session.openServiceAsync(service_name)

        # wait until ServiceOpened event is received
        await self.services[service_name].wait()

        service = self.session.getService(service_name)
        return service

    @staticmethod
    def _close_requests(requests: List[ReferenceDataRequest]):
        """
        Notify requests that their last event was received (i.e., send None to
        their queue)
        """
        for request in requests:
            request.send_queue_message(None)

    def _is_error_msg(self, msg: blpapi.Message) -> bool:
        """
        Return True if msg contains responseError element. It indicates errors
        such as lost connection, request limit reached etc.
        """
        if msg.hasElement(RESPONSE_ERROR):
            requests = [self.requests[cor_id]
                        for cor_id in msg.correlationIds()]
            self._close_requests(requests)

            LOGGER.debug('%s: error message received: %s',
                         self.__class__.__name__,
                         msg.getElement(RESPONSE_ERROR))
            return True

        return False

    def _session_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SESSION_STATUS events. If session is successfully
        started, set `self.connection`
        """
        msg = list(event_)[0]

        if msg.asElement().name() == 'SessionStarted':
            LOGGER.debug('%s: session opened', self.__class__.__name__)
            self.loop.call_soon_threadsafe(self.connection.set)

    def _service_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SERVICE_STATUS events. If service is successfully
        started, set corresponding event in `self.services`
        """
        msg = list(event_)[0]

        # todo check which service was actually opened
        if msg.asElement().name() == 'ServiceOpened':
            for service_name, service_event in self.services.items():

                LOGGER.debug('%s: service %s opened',
                             self.__class__.__name__,
                             service_name)
                self.loop.call_soon_threadsafe(service_event.set)

    def _partial_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.PARTIAL_RESPONSE events. Send all valid messages
        from the given event to the requests with the corresponding
        correlation id
        """
        for msg in event_:

            if self._is_error_msg(msg):
                continue

            for cor_id in msg.correlationIds():
                request = self.requests[cor_id]
                request.send_queue_message(msg)

    def _response_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.RESPONSE events. This is the last event for the
        corresponding requests, therefore after processing all messages
        from the event, None will be send to the corresponding requests.
        """
        self._partial_handler(event_)

        for msg in event_:
            requests = [self.requests[cor_id]
                        for cor_id in msg.correlationIds()]

            self._close_requests(requests)

    def __call__(self, event: blpapi.Event, session: Optional[blpapi.Session]):
        """
        This method is called from Bloomberg session in a separate thread
        for each incoming event.
        """
        LOGGER.debug('%s: event with type %s received',
                     self.__class__.__name__,
                     event.eventType())
        self.method_map[event.eventType()](event)
