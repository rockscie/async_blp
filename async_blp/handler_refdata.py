"""
File contains handler for ReferenceDataRequest
"""

import asyncio
import uuid
from collections import defaultdict
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

from async_blp.requests import ReferenceDataSubscribe
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


class Handler(AbsHandler):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session

    Base for subscribe and request
    Work with sessions and services
    """

    def __call__(self, event: blpapi.Event, session: Optional[blpapi.Session]):
        """
        This method is called from Bloomberg session in a separate thread
        for each incoming event.
        """
        LOGGER.debug('%s: event with type %s received',
                     self.__class__.__name__,
                     event.eventType())
        self._method_map[event.eventType()](event)

    def __init__(self,
                 session_options: blpapi.SessionOptions,
                 loop: asyncio.AbstractEventLoop = None):
        """
        _current_requests -  can contains bloomberg requests and
        subscriptions in processing

        _services - all open services

        _method_map -  manufactures for processing bloomberg event
        """

        self._current_requests: Dict[blpapi.CorrelationId,
                                     ReferenceDataRequest] = {}
        super().__init__(loop)
        self._session = blpapi.Session(options=session_options,
                                       eventHandler=self)
        # It is important to start session with startAsync before doing anything
        # else
        self._session.startAsync()
        LOGGER.debug('%s: session started', self.__class__.__name__)
        # loop is used for internal coordination

        self._services: Dict[str,
                             asyncio.Event] = defaultdict(lambda:
                                                          asyncio.Event(
                                                              loop=self._loop)
                                                          )

        # each event type is processed by its own method full description
        # 9.2 BLPAPI-Core-Developer-Guide
        self._method_map: Dict[int, Callable] = {
            blpapi.Event.SESSION_STATUS:       self._session_handler,
            blpapi.Event.SERVICE_STATUS:       self._service_handler,
            blpapi.Event.ADMIN:                self._admin_handler,
            blpapi.Event.AUTHORIZATION_STATUS: self._raise_unknown_msg,
            blpapi.Event.RESOLUTION_STATUS:    self._raise_unknown_msg,
            blpapi.Event.TOPIC_STATUS:         self._raise_unknown_msg,
            blpapi.Event.TOKEN_STATUS:         self._raise_unknown_msg,
            blpapi.Event.REQUEST:              self._raise_unknown_msg,
            blpapi.Event.UNKNOWN:              self._raise_unknown_msg
            }

    def stop_session(self):
        """
        Close all requests and begin the process to stop session.
        Application must wait for the `session_stopped` event to be set
        before
        deleting this handler, otherwise the main thread can hang forever
        """
        self._close_requests(self._current_requests.keys())
        self._session.stopAsync()

    def _close_requests(self, corr_ids: Iterable[blpapi.CorrelationId]):
        """
        Notify requests that their last event was sent (i.e., send None to
        their queue) and delete from requests dict
        """
        for corr_id in corr_ids:
            request = self._current_requests[corr_id]
            request.send_queue_message(None)
            del self._current_requests[corr_id]

    @property
    def get_current_weight(self):
        """
        score for load balance, all _current_requests are equal
        """

        return sum(request.weight
                   for request in self._current_requests.values())

    async def _get_service(self, service_name: str) -> blpapi.Service:
        """
        Try to open service if it wasn't opened yet. Session must be opened
        before calling this method
        """
        service_event = self._services[service_name]
        if not service_event.is_set():
            self._session.openServiceAsync(service_name)

        # wait until ServiceOpened event is received
        await self._services[service_name].wait()

        service = self._session.getService(service_name)
        return service

    @staticmethod
    def _raise_unknown_msg(msg: Union[blpapi.Message,
                                      blpapi.Event]):
        """
        try find unimplemented methods
        """
        if isinstance(msg, blpapi.Event):
            LOGGER.debug('unknown event type: %s', msg.eventType())
            msg = list(msg)[0]
        LOGGER.debug(msg)
        raise ValueError(f"please in add {msg.asElement().name()}")

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

        elif msg.asElement().name() == 'SessionTerminated':
            LOGGER.debug('%s: session stopped', self.__class__.__name__)
            self._loop.call_soon_threadsafe(self.session_stopped.set)
        elif msg.asElement().name() == 'SessionConnectionUp':
            LOGGER.debug('%s: session started', self.__class__.__name__)
        elif msg.asElement().name() == 'SessionConnectionDown':
            LOGGER.debug('%s: SessionConnectionDown', self.__class__.__name__)
            self._loop.call_soon_threadsafe(self.session_stopped.set)
        else:
            # SessionStartupFailure
            self._raise_unknown_msg(msg)

    def _service_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SERVICE_STATUS events. If service is successfully
        started, set corresponding event in `self.services`
        """
        msg = list(event_)[0]

        if msg.asElement().name() == 'ServiceOpened':
            service_name = msg.getElement('serviceName').getValue()
            service_event = self._services[service_name]

            LOGGER.debug('%s: service %s opened',
                         self.__class__.__name__,
                         service_name)
            self._loop.call_soon_threadsafe(service_event.set)
        else:
            # SessionClusterInfo
            # SessionClusterInfo
            # ServiceOpenFailure
            self._raise_unknown_msg(msg)

    def _admin_handler(self, event_):
        """
        Process blpapi.Event.ADMIN events.
        Process all msg with warning about Data Loss
        """
        for msg in event_:
            if msg.asElement().name() == 'SlowConsumerWarning':
                LOGGER.debug('%s: sIndicates client is slow. '
                             'NO category/subcategory',
                             self.__class__.__name__)
                LOGGER.debug(msg)
            elif msg.asElement().name() == 'SlowConsumerWarningCleared':
                LOGGER.debug('%s:Indicates client is slow. '
                             'NO category/subcategory',
                             self.__class__.__name__)
            elif msg.asElement().name() == 'DataLoss':
                LOGGER.debug('%s: we have loose data',
                             self.__class__.__name__)
            elif msg.asElement().name() in (
                    'RequestTemplateAvailable',
                    'RequestTemplatePending',
                    'RequestTemplateTerminated',):
                LOGGER.debug('%s: we have loose data', self.__class__.__name__)
            else:
                self._raise_unknown_msg(msg)


class RequestHandler(Handler):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session

    Work with historical and current Requests
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions,
                 loop: asyncio.AbstractEventLoop = None):

        super().__init__(session_options, loop)

        # each event type is processed by its own method full description
        # 9.2 BLPAPI-Core-Developer-Guide

        self._method_map[blpapi.Event.RESPONSE] = self._response_handler
        self._method_map[blpapi.Event.PARTIAL_RESPONSE] = self._partial_handler
        self._method_map[blpapi.Event.REQUEST_STATUS] = self._raise_unknown_msg
        self._method_map[blpapi.Event.TIMEOUT] = self._raise_unknown_msg

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


class SubHandler(Handler):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session

    Work with subscribes
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions,
                 loop: asyncio.AbstractEventLoop = None):
        super().__init__(session_options, loop)

        self._current_requests: Dict[str,
                                     ReferenceDataSubscribe] = {}

        self._method_map[
            blpapi.Event.SUBSCRIPTION_STATUS] = self._raise_unknown_msg
        self._method_map[
            blpapi.Event.SUBSCRIPTION_DATA] = self._subscriber_data_handler

    def _subscriber_data_handler(self, event_: blpapi.Event):
        """
        new data in subscriber
        """
        for msg in event_:
            for _ in msg.correlationIds():
                for request in self._current_requests.values():
                    request.send_queue_message(msg)

    def _subscriber_status_handler(self, event_: blpapi.Event):
        """
        if all ok do nothing
        """
        for msg in event_:
            if msg.asElement().name() not in ("SubscriptionStarted",
                                              "SubscriptionStreamsActivated",
                                              ):
                self._raise_unknown_msg(msg)

    async def subscribe(self, subscribes: List[ReferenceDataSubscribe]):
        """
        Send requests to Bloomberg

        Wait until session is started and required service is opened,
        then send requests
        """
        await self.session_started.wait()

        for subscribe in subscribes:
            corr_id = str(uuid.uuid4())
            self._current_requests[corr_id] = subscribe
            blp_subscribe = subscribe.create()
            self._session.subscribe(blp_subscribe, requestLabel=corr_id)
            LOGGER.debug('%s: subscribe send:\n%s',
                         self.__class__.__name__,
                         blp_subscribe)
