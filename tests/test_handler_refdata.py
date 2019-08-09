"""
For test Handler we must use env_test for emulate Bloomberg
"""
import asyncio

import pytest

from async_blp.env_test import Element
from async_blp.env_test import Event
from async_blp.env_test import Message
from async_blp.env_test import SessionOptions
from async_blp.handler_refdata import HandlerRef
from async_blp.requests import ReferenceDataRequest

# pylint is not like pytest.fixture but we do
# pylint: disable=redefined-outer-name
# it's test we need protected-access
# pylint: disable=protected-access


@pytest.fixture()
def session_options_bl() -> SessionOptions():
    """
    for test not important
    """
    session_options_ = SessionOptions()
    session_options_.setServerHost("localhost")
    session_options_.setServerPort(8194)
    return session_options_


@pytest.fixture()
def request_bl():
    """
    Simple request
    """
    field_name = 'PX_LAST'
    security_id = 'F Equity'
    return ReferenceDataRequest([security_id], [field_name])


@pytest.fixture()
def element_daily_reached():
    """
    error when you load to many
    """
    return Element(name='subcategory', value='DAILY_LIMIT_REACHED')


@pytest.fixture()
def element_monthly_reached():
    """
    error when you load to many
    """
    return Element(name='subcategory', value='MONTHLY_LIMIT_REACHED')


@pytest.fixture()
def msg_daily_reached():
    """
    error when you load to many
    """
    return Message(name="responseError",
                   value='',
                   children={
                       "responseError": element_daily_reached
                       }
                   )


@pytest.fixture()
def error_event(msg_daily_reached):
    """
    error when you load to many
    """
    return Event(type_=Event.RESPONSE,
                 msgs=[
                     msg_daily_reached,
                     ],
                 )


@pytest.mark.asyncio
@pytest.mark.timeout(5)
class TestHandlerAsync:
    """
    test all async methods
    """

    async def test_start_connection(self, session_options_bl):
        """
        we try connect in init
        """
        handler = HandlerRef(session_options_bl)
        await handler.connection.wait()
        assert handler.connection.is_set()

    async def test_send(self, session_options_bl, request_bl):
        """
        only handler knows when we can open Service
        """
        request_bl.loop = asyncio.get_running_loop()
        handler = HandlerRef(session_options_bl)
        await handler.send_requests([request_bl])
        await handler.send_requests([request_bl])
        assert handler.requests
        assert len(handler.requests) > 1

    async def test_get_service(self, session_options_bl):
        """
        only handler knows when we can open Service
        """
        handler = HandlerRef(session_options_bl)
        assert await handler._get_service('test')
        assert handler.services['test'].is_set()

    async def test_call_limit(self, session_options_bl,
                              request_bl,
                              msg_daily_reached, ):
        """
        only handler knows when we can open Service
        """
        handler = HandlerRef(session_options_bl)
        request_bl.loop = asyncio.get_running_loop()
        handler.requests[None] = request_bl
        handler._is_error_msg(msg_daily_reached)
        assert await request_bl.msg_queue.get() is None

    @pytest.mark.skip()
    async def test_star_stop(self, session_options_bl, request_bl):
        """
        Just open service and wait for RESPONSE
        """

        request_bl.loop = asyncio.get_running_loop()
        handler = HandlerRef(session_options_bl)
        asyncio.create_task(handler.send_requests([request_bl]))
        asyncio.create_task(request_bl.process())
        await request_bl.process()

        assert True
