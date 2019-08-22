"""
abstract Handler for typing
"""

import asyncio
from collections import defaultdict
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Union

from .base_request import RequestBase
from .utils import log
from .utils.exc import BloombergException

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

LOGGER = log.get_logger()


class HandlerBase:
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session

    Base class for request and subscription handlers. Processes common session,
    service and admin events
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

        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError('Please create handler inside asyncio loop'
                               'or explicitly provide one')

        # asyncio events to signal session start/stop
        self.session_started = asyncio.Event()
        self.session_stopped = asyncio.Event()

        # Bloomberg session, each session get its own handler instance
        self._session = blpapi.Session(options=session_options,
                                       eventHandler=self)
        self._session.startAsync()
        LOGGER.debug('%s: session started', self.__class__.__name__)

        # requests that are currently in process
        self._current_requests: Dict[blpapi.CorrelationId, RequestBase] = {}

        # all opened services; used to signal when service is ready to be used
        self._services: Dict[str,
                             asyncio.Event] = defaultdict(lambda:
                                                          asyncio.Event(
                                                              loop=self._loop)
                                                          )

        # each event type is processed by its own method
        # for event description see BLPAPI-Core-Developer-Guide, section 9.2
        self._method_map: Dict[int, Callable[[blpapi.Event], None]] = {
            blpapi.Event.SESSION_STATUS:       self._session_handler,
            blpapi.Event.SERVICE_STATUS:       self._service_handler,
            blpapi.Event.ADMIN:                self._admin_handler,
            blpapi.Event.AUTHORIZATION_STATUS: self._raise_exception,
            blpapi.Event.RESOLUTION_STATUS:    self._raise_exception,
            blpapi.Event.TOPIC_STATUS:         self._raise_exception,
            blpapi.Event.TOKEN_STATUS:         self._raise_exception,
            blpapi.Event.REQUEST:              self._raise_exception,
            blpapi.Event.UNKNOWN:              self._raise_exception
            }

    def stop_session(self):  # pragma: no cover
        """
        Close all requests and begin the process to stop session.
        Application must wait for the `session_stopped` event to be set
        before deleting this handler, otherwise the main thread can hang forever
        """
        self._close_requests(self._current_requests.keys())
        self._session.stopAsync()

    def _close_requests(self, corr_ids: Iterable[blpapi.CorrelationId]):
        """
        Notify requests that their last event was sent (i.e., send None to
        their queue) and delete from current requests dict
        """
        for corr_id in corr_ids:

            # this is strictly for testing
            if corr_id is None:  # pragma: no cover
                for request in self._current_requests.values():
                    request.send_queue_message(None)

            try:
                request = self._current_requests.pop(corr_id)
            except KeyError:  # pragma: no cover
                continue
            else:
                request.send_queue_message(None)

    @property
    def current_load(self):
        """
        Estimate this handler's current load; used to balance load between
        handlers
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
    def _raise_exception(msg: Union[blpapi.Message,
                                    blpapi.Event]):
        """
        Log errors ans raise suitable exception
        """
        if isinstance(msg, blpapi.Event):
            LOGGER.debug('unknown event type: %s', msg.eventType())
            msg = list(msg)[0]

        LOGGER.debug(msg)
        raise BloombergException(f"Unknown message: {msg.asElement().name()}")

    def _session_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SESSION_STATUS events.
        If session is successfully started, set `self.session_started`
        If session is successfully stopped, set `self.session_stopped`
        """
        msg = list(event_)[0]
        msg_name = msg.asElement().name()

        if msg_name == 'SessionStarted':
            LOGGER.debug('%s: session opened', self.__class__.__name__)
            self._loop.call_soon_threadsafe(self.session_started.set)

        elif msg_name == 'SessionTerminated':
            LOGGER.debug('%s: session stopped', self.__class__.__name__)
            self._loop.call_soon_threadsafe(self.session_stopped.set)

        elif msg_name in {'SessionConnectionUp',
                          'SessionConnectionDown'}:  # pragma: no cover
            LOGGER.debug('%s: %s', self.__class__.__name__, msg_name)

        elif msg_name in {'SessionClusterInfo',
                          'SessionClusterUpdate'}:  # pragma: no cover
            # maybe do smth meaningful in the future
            pass

        else:
            # SessionStartupFailure
            self._raise_exception(msg)

    def _service_handler(self, event_: blpapi.Event):
        """
        Process blpapi.Event.SERVICE_STATUS events. If service is successfully
        started, set corresponding event in `self.services`
        """
        msg = list(event_)[0]

        msg_name = msg.asElement().name()

        if msg_name == 'ServiceOpened':
            service_name = msg.getElement('serviceName').getValue()
            service_event = self._services[service_name]

            LOGGER.debug('%s: service %s opened',
                         self.__class__.__name__,
                         service_name)
            self._loop.call_soon_threadsafe(service_event.set)

        else:
            # ServiceOpenedFailure
            self._raise_exception(msg)

    def _admin_handler(self, event_):  # pragma: no cover
        """
        Process blpapi.Event.ADMIN events. This includes warnings about slow
        consumer and possible data loss
        """

        for msg in event_:
            msg_name = msg.asElement().name()

            if msg_name == 'SlowConsumerWarning':
                LOGGER.warning('%s: Client is slow.',
                               self.__class__.__name__)
                LOGGER.debug(msg)

            elif msg_name == 'SlowConsumerWarningCleared':
                LOGGER.warning('%s: Client is not slow anymore',
                               self.__class__.__name__)
                LOGGER.debug(msg)

            elif msg_name == 'DataLoss':
                LOGGER.warning('%s: some data have been lost due to event '
                               'queue overflowing',
                               self.__class__.__name__)
                LOGGER.debug(msg)

            elif msg_name in ('RequestTemplateAvailable',
                              'RequestTemplatePending',
                              'RequestTemplateTerminated'):
                LOGGER.debug(msg)

            else:
                self._raise_exception(msg)
