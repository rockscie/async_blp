"""
Test handler for ReferenceDataRequest
"""
import asyncio

import pytest

from async_blp.handler_refdata import HandlerRef


# pylint does not like pytest.fixture but we do
# pylint: disable=redefined-outer-name

# we need protected access in tests
# pylint: disable=protected-access


@pytest.mark.asyncio
@pytest.mark.timeout(11)
class TestHandleRef:
    """
    test all async methods
    """

    async def test___session_handler__start_session__single_thread(
            self,
            session_options,
            open_session_event,
            ):
        """
        Check that receiving `SessionStarted` blpapi event correctly sets the
        `HandlerRef.session_started` asyncio event
        """
        handler = HandlerRef(session_options)
        assert not handler.session_started.is_set()

        # in blpapi this happens under the hood
        handler._session_handler(open_session_event)

        await handler.session_started.wait()
        assert handler.session_started.is_set()

    async def test__session_handler__start_session__multi_thread(
            self,
            session_options,
            open_session_event,
            ):
        """
        Check that receiving `SessionStarted` blpapi event correctly sets the
        `HandlerRef.session_started` asyncio event when handler is called from
        a different thread
        """
        handler = HandlerRef(session_options)
        assert not handler.session_started.is_set()

        # in blpapi this happens under the hood
        handler.session.send_event(open_session_event)

        await handler.session_started.wait()
        assert handler.session_started.is_set()

    async def test___session_handler__stop_session(
            self,
            session_options,
            stop_session_event,
            ):
        """
        Check that receiving `SessionStopped` blpapi event correctly sets the
        `HandlerRef.session_stopped` asyncio event
        """
        handler = HandlerRef(session_options)
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
        handler = HandlerRef(session_options)
        handler.services['test'] = asyncio.Event()

        handler.session.send_event(open_service_event)
        await handler.services['test'].wait()
        assert handler.services['test'].is_set()

    async def test__service_handler__multi_thread(self,
                                                  session_options,
                                                  open_service_event):
        """
        Check that receiving `ServiceOpened` blpapi event correctly sets the
        corresponding asyncio event when handler is called from
        a different thread
        """
        handler = HandlerRef(session_options)
        handler.services['test'] = asyncio.Event()

        handler.session.send_event(open_service_event)
        await handler.services['test'].wait()
        assert handler.services['test'].is_set()

    async def test__get_service(self,
                                session_options,
                                open_service_event,
                                ):
        """
        Check that `get_service()` waits for the correct event
        """
        handler = HandlerRef(session_options)
        task = asyncio.create_task(handler._get_service('test'))
        await asyncio.sleep(0.00001)

        handler.session.send_event(open_service_event)
        assert await task
        assert handler.services['test'].is_set()

    async def test__send_requests__correlation_id(self,
                                    session_options,
                                    data_request,
                                    ):
        """
        Check that different correlation id is created for each request
        """
        handler = HandlerRef(session_options)
        handler.session_started.set()

        task = asyncio.create_task(handler.send_requests([data_request]))
        task1 = asyncio.create_task(handler.send_requests([data_request]))

        data_request.loop = asyncio.get_running_loop()
        await asyncio.sleep(0.00001)
        assert len(handler.requests) > 1, "all requests must have their own id"

        task.cancel()
        task1.cancel()

    async def test_call_limit(self,
                              session_options,
                              data_request,
                              msg_daily_reached,
                              ):
        """
        @georgy please clarify what it does
        """

        handler = HandlerRef(session_options)
        data_request.set_running_loop_as_default()
        handler._close_requests([data_request])
        assert await data_request._msg_queue.get() is None

        handler.requests[None] = data_request
        handler._is_error_msg(msg_daily_reached)
        assert await data_request._msg_queue.get() is None
