"""
File contains handler for ReferenceDataRequest
"""

import asyncio
import uuid
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from .abs_handler import AbsHandler
from .requests import ReferenceDataRequest
from .utils.blp_name import RESPONSE_ERROR
from .utils.log import get_logger

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

LOGGER = get_logger()


class RequestHandler(AbsHandler):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions,
                 loop: asyncio.AbstractEventLoop = None):

        super().__init__()
        self._current_requests: Dict[blpapi.CorrelationId,
                                     ReferenceDataRequest] = {}
        self.session_started = asyncio.Event()
        self.session_stopped = asyncio.Event()
        self._services: Dict[str, asyncio.Event] = {}

        self._session = blpapi.Session(options=session_options,
                                       eventHandler=self)

        # It is important to start session with startAsync before doing anything
        # else
        self._session.startAsync()
        LOGGER.debug('%s: session started', self.__class__.__name__)

        # loop is used for internal coordination
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError('Please create handler inside asyncio loop'
                               'or explicitly provide one')

        # each event type is processed by its own method
        self._method_map = {
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
        await self.session_started.wait()

        for request in requests:
            corr_id = blpapi.CorrelationId(uuid.uuid4())
            self._current_requests[corr_id] = request

            # wait until the necessary service is opened
            service = await self._get_service(request.service_name)

            blp_request = request.create(service)
            self._session.sendRequest(blp_request, correlationId=corr_id)
            LOGGER.debug('%s: request send:\n%s',
                         self.__class__.__name__,
                         blp_request)

    async def _get_service(self, service_name: str) -> blpapi.Service:
        """
        Try to open service if it wasn't opened yet. Session must be opened
        before calling this method
        """
        if service_name not in self._services:
            self._services[service_name] = asyncio.Event()
            self._session.openServiceAsync(service_name)

        # wait until ServiceOpened event is received
        await self._services[service_name].wait()

        service = self._session.getService(service_name)
        return service

    def _close_requests(self, corr_ids: Iterable[blpapi.CorrelationId]):
        """
        Notify requests that their last event was sent (i.e., send None to
        their queue) and delete from requests dict
        """
        for corr_id in corr_ids:
            request = self._current_requests[corr_id]
            request.send_queue_message(None)

            del self._current_requests[corr_id]

    @classmethod
    def _is_error_msg(cls, msg: blpapi.Message) -> bool:
        """
        Return True if msg contains responseError element. It indicates errors
        such as lost connection, request limit reached etc.
        """
        if msg.hasElement(RESPONSE_ERROR):

            LOGGER.debug('%s: error message received:\n%s',
                         cls.__name__,
                         msg)
            return True

        return False

    def _session_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SESSION_STATUS events.
        If session is successfully started, set `self.session_started`
        If session is successfully stopped, set `self.session_stopped`
        """
        msg = list(event_)[0]

        if msg.asElement().name() == 'SessionStarted':
            LOGGER.debug('%s: session opened', self.__class__.__name__)
            self._loop.call_soon_threadsafe(self.session_started.set)

        if msg.asElement().name() == 'SessionTerminated':
            LOGGER.debug('%s: session stopped', self.__class__.__name__)
            self._loop.call_soon_threadsafe(self.session_stopped.set)

    def _service_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SERVICE_STATUS events. If service is successfully
        started, set corresponding event in `self.services`
        """
        msg = list(event_)[0]

        # todo check which service was actually opened
        if msg.asElement().name() == 'ServiceOpened':
            for service_name, service_event in self._services.items():

                LOGGER.debug('%s: service %s opened',
                             self.__class__.__name__,
                             service_name)
                self._loop.call_soon_threadsafe(service_event.set)

    def _partial_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.PARTIAL_RESPONSE events. Send all valid messages
        from the given event to the requests with the corresponding
        correlation id
        """
        for msg in event_:

            if self._is_error_msg(msg):
                self._close_requests(msg.correlationIds())
                continue

            for cor_id in msg.correlationIds():
                request = self._current_requests[cor_id]
                request.send_queue_message(msg)

    def _response_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.RESPONSE events. This is the last event for the
        corresponding requests, therefore after processing all messages
        from the event, None will be send to the corresponding requests.
        """
        self._partial_handler(event_)

        for msg in event_:
            self._close_requests(msg.correlationIds())

    def __call__(self, event: blpapi.Event, session: Optional[blpapi.Session]):
        """
        This method is called from Bloomberg session in a separate thread
        for each incoming event.
        """
        LOGGER.debug('%s: event with type %s received',
                     self.__class__.__name__,
                     event.eventType())
        self._method_map[event.eventType()](event)

    def stop_session(self):
        """
        Close all requests and begin the process to stop session.
        Application must wait for the `session_stopped` event to be set before
        deleting this handler, otherwise the main thread can hang forever
        """
        self._close_requests(self._current_requests.keys())
        self._session.stopAsync()

    def get_current_weight(self):
        return sum(request.weight
                   for request in self._current_requests.values())
