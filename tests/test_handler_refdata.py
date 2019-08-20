"""
Test handler for ReferenceDataRequest
"""
import asyncio
import uuid

import pytest

from async_blp.handler_refdata import RequestHandler
from async_blp.handler_refdata import SubscriptionHandler
from async_blp.requests import Subscription
from async_blp.utils.env_test import CorrelationId
from async_blp.utils.env_test import Message


# pylint does not like pytest.fixture but we do
# pylint: disable=redefined-outer-name
# we need protected access in tests
# pylint: disable=protected-access


@pytest.mark.asyncio
@pytest.mark.timeout(11)
class TestRequestHandler:
    """
    test all async methods in base Handler and Request
    """

    async def test___session_handler__start_session__single_thread(
            self,
            session_options,
            open_session_event, ):
        """
        Check that receiving `SessionStarted` blpapi event correctly sets the
        `HandlerRef.session_started` asyncio event
        """
        handler = RequestHandler(session_options)
        assert not handler.session_started.is_set()

        # in blpapi this happens under the hood
        handler._session_handler(open_session_event)

        await handler.session_started.wait()
        assert handler.session_started.is_set()

    async def test__session_handler__start_session__multi_thread(
            self,
            session_options,
            open_session_event, ):
        """
        Check that receiving `SessionStarted` blpapi event correctly sets the
        `HandlerRef.session_started` asyncio event when handler is called from
        a different thread
        """
        handler = RequestHandler(session_options)
        assert not handler.session_started.is_set()

        # in blpapi this happens under the hood
        handler._session.send_event(open_session_event)

        await handler.session_started.wait()
        assert handler.session_started.is_set()

    async def test___session_handler__stop_session(
            self,
            session_options,
            stop_session_event, ):
        """
        Check that receiving `SessionStopped` blpapi event correctly sets the
        `HandlerRef.session_stopped` asyncio event
        """
        handler = RequestHandler(session_options)
        assert not handler.session_stopped.is_set()

        # in blpapi this happens under the hood
        handler._session_handler(stop_session_event)

        await handler.session_stopped.wait()
        assert handler.session_stopped.is_set()

    async def test__service_handler__single_thread(self,
                                                   session_options,
                                                   open_service_event):
        """
        Check that receiving `ServiceOpened` blpapi event correctly sets the
        corresponding asyncio event
        """
        handler = RequestHandler(session_options)
        handler._services['//blp/refdata'] = asyncio.Event()

        handler._session.send_event(open_service_event)

        await handler._services['//blp/refdata'].wait()
        assert handler._services['//blp/refdata'].is_set()

    async def test__service_handler__multi_thread(self,
                                                  session_options,
                                                  open_service_event):
        """
        Check that receiving `ServiceOpened` blpapi event correctly sets the
        corresponding asyncio event when handler is called from
        a different thread
        """
        handler = RequestHandler(session_options)
        handler._services['//blp/refdata'] = asyncio.Event()

        handler._session.send_event(open_service_event)
        await handler._services['//blp/refdata'].wait()
        assert handler._services['//blp/refdata'].is_set()

    async def test__get_service(self,
                                session_options,
                                open_service_event,
                                ):
        """
        Check that `get_service()` waits for the correct event
        """
        handler = RequestHandler(session_options)
        task = asyncio.create_task(handler._get_service('//blp/refdata'))
        await asyncio.sleep(0.00001)

        handler._session.send_event(open_service_event)
        assert await task
        assert handler._services['//blp/refdata'].is_set()

    async def test__send_requests__correlation_id(self,
                                                  session_options,
                                                  data_request,
                                                  ):
        """
        Check that different correlation id is created for each request
        """
        handler = RequestHandler(session_options)
        handler.session_started.set()

        task = asyncio.create_task(handler.send_requests([data_request]))
        task1 = asyncio.create_task(handler.send_requests([data_request]))

        data_request.loop = asyncio.get_running_loop()
        await asyncio.sleep(0.00001)
        assert len(
            handler._current_requests) > 1, "all requests must have their own " \
                                            "" \
                                            "" \
                                            "" \
                                            "" \
                                            "" \
                                            "" \
                                            "" \
                                            "" \
                                            "id"

        task.cancel()
        task1.cancel()

    async def test___close_requests(self,
                                    session_options,
                                    data_request,
                                    ):
        """
        `close_requests` should send `None` to request's queue and remove
        request from current requests dict
        """

        handler = RequestHandler(session_options)
        data_request.set_running_loop_as_default()

        corr_id = CorrelationId(uuid.uuid4())
        handler._current_requests[corr_id] = data_request

        handler._close_requests([corr_id])

        assert await data_request._msg_queue.get() is None
        assert not handler._current_requests

    async def test__is_error_msg__daily_limit(self,
                                              msg_daily_reached,
                                              ):
        assert RequestHandler._is_error_msg(msg_daily_reached)

    async def test__is_error_msg__non_error_msg(self,
                                                non_error_message,
                                                ):
        assert not RequestHandler._is_error_msg(non_error_message)


@pytest.mark.asyncio
@pytest.mark.timeout(11)
class TestSubHandler:
    """
    test async method in subscriber
    """

    async def test___subscriber_data_handler__start(self,
                                                    session_options,
                                                    start_subscribe_event):
        """
        ignore start subscriber
        """
        s_handler = SubscriptionHandler(session_options)
        s_handler._subscriber_status_handler(start_subscribe_event)
        assert True

    async def test___subscriber_data_handler__wrong_msg(self,
                                                        session_options,
                                                        market_data_event):
        """
        raise wrong event
        """
        s_handler = SubscriptionHandler(session_options)
        name = list(market_data_event)[0].name()
        print(name)
        with pytest.raises(ValueError) as excinfo:
            s_handler._subscriber_status_handler(market_data_event)
        assert name in str(excinfo.value)

    async def test__subscriber_data_handler__start(self,
                                                   session_options,
                                                   market_data_event, ):
        """
        put data in queue
        """
        security_id = 'F Equity'
        field_name = 'MKTDATA'
        sub = Subscription([security_id],
                           [field_name])
        msg: Message = list(market_data_event)[0]
        cor_id = list(msg.correlationIds())[0]
        sub._security_mapping[cor_id] = security_id

        s_handler = SubscriptionHandler(session_options)
        s_handler.session_started.set()
        await s_handler.subscribe([sub])
        await asyncio.sleep(0.00001)
        s_handler._subscriber_data_handler(market_data_event)
        await asyncio.sleep(0.00001)

        assert not sub._msg_queue.empty()
