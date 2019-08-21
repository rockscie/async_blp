"""
Test handler for ReferenceDataRequest
"""
import asyncio

import pytest

from async_blp.handlers import RequestHandler
from async_blp.handlers import SubscriptionHandler
from async_blp.requests import Subscription
from async_blp.utils.env_test import Message
from async_blp.utils.exc import BloombergException


# we need protected access in tests
# pylint: disable=protected-access


@pytest.mark.asyncio
@pytest.mark.timeout(11)
class TestRequestHandler:
    """
    test all async methods in base Handler and Request
    """

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
                                            "" \
                                            "" \
                                            "" \
                                            "" \
                                            "id"

        task.cancel()
        task1.cancel()

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
class TestSubscriptionHandler:
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

        with pytest.raises(BloombergException) as excinfo:
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
