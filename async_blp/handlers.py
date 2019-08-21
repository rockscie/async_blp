"""
File contains handler for ReferenceDataRequest
"""

import asyncio
import uuid
from typing import Dict
from typing import List

from .base_handler import HandlerBase
from .base_request import RequestBase
from .requests import Subscription
from .utils.blp_name import RESPONSE_ERROR
from .utils.log import get_logger

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

LOGGER = get_logger()


class RequestHandler(HandlerBase):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session

    Sends requests and processes incoming responses.
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions,
                 loop: asyncio.AbstractEventLoop = None):

        super().__init__(session_options, loop)

        local_methods = {
            blpapi.Event.RESPONSE:         self._response_handler,
            blpapi.Event.PARTIAL_RESPONSE: self._partial_response_handler,

            # according to BLPAPI-Core-Developer-Guide section 10.1,
            # REQUEST_STATUS event is send only with RequestFailure messages
            blpapi.Event.REQUEST_STATUS:   self._raise_exception,
            }

        self._method_map.update(local_methods)

    async def send_requests(self, requests: List[RequestBase]):
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

    def _partial_response_handler(self, event_: blpapi.Event):
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
        self._partial_response_handler(event_)

        for msg in event_:
            self._close_requests(msg.correlationIds())


class SubscriptionHandler(HandlerBase):
    """
    Handler gets response events from Bloomberg from other thread,
    then puts it to request queue. Each handler opens its own session

    Used for handling subscription requests and responses
    """

    def __init__(self,
                 session_options: blpapi.SessionOptions,
                 loop: asyncio.AbstractEventLoop = None):
        super().__init__(session_options, loop)

        # only for typing
        self._current_requests: Dict[str, Subscription] = {}

        local_methods = {
            blpapi.Event.SUBSCRIPTION_STATUS: self._subscriber_status_handler,
            blpapi.Event.SUBSCRIPTION_DATA:   self._subscriber_data_handler,
            }

        self._method_map.update(local_methods)

    def _subscriber_data_handler(self, event_: blpapi.Event):
        """
        Redirect data to the request queue.

        TODO: Currently, we just send all received data to all requests.
        """
        for msg in event_:
            for request in self._current_requests.values():
                request.send_queue_message(msg)

    def _subscriber_status_handler(self, event_: blpapi.Event):
        """
        Raise exception if something goes wrong
        """
        for msg in event_:
            if msg.asElement().name() not in ("SubscriptionStarted",
                                              "SubscriptionStreamsActivated",
                                              ):
                self._raise_exception(msg)

    async def subscribe(self, subscriptions: List[Subscription]):
        """
        Send subscriptions to Bloomberg

        Wait until session is started, then send subscription
        """
        await self.session_started.wait()

        for subscription in subscriptions:
            corr_id = str(uuid.uuid4())
            self._current_requests[corr_id] = subscription

            blp_subscription = subscription.create_subscription()
            self._session.subscribe(blp_subscription, requestLabel=corr_id)

            LOGGER.debug('%s: subscription send:\n%s',
                         self.__class__.__name__,
                         blp_subscription)

    async def read_subscribers(self):
        tasks = [asyncio.create_task(request.process())
                 for request in self._current_requests.values()]

        requests_result = await asyncio.gather(*tasks)
        return requests_result
