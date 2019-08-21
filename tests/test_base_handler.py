import asyncio
import uuid

import pytest

from async_blp.base_handler import HandlerBase
from async_blp.requests import ReferenceDataRequest
from async_blp.utils.env_test import CorrelationId
from async_blp.utils.exc import BloombergException


class TestHandlerBase:

    def test___init___not_inside_loop(self, session_options):
        with pytest.raises(RuntimeError):
            HandlerBase(session_options)

    # def test__stop_session(self, session_options, open_session_event):
    #     handler = HandlerBase(session_options)

    async def test__current_load(self, session_options):
        handler = HandlerBase(session_options)
        request = ReferenceDataRequest(['security'],
                                       ['field1', 'field2'])
        handler._current_requests['id'] = request

        assert handler.current_load == 2

    async def test___raise_exception__message(self, msg_daily_reached,
                                              session_options):
        handler = HandlerBase(session_options)
        with pytest.raises(BloombergException):
            handler._raise_exception(msg_daily_reached)

    async def test__raise_exception__event(self, response_event,
                                           session_options):
        handler = HandlerBase(session_options)
        with pytest.raises(BloombergException):
            handler._raise_exception(response_event)

    async def test___close_requests(self,
                                    session_options,
                                    data_request,
                                    ):
        """
        `close_requests` should send `None` to request's queue and remove
        request from current requests dict
        """

        handler = HandlerBase(session_options)
        data_request.set_running_loop_as_default()

        corr_id = CorrelationId(uuid.uuid4())
        handler._current_requests[corr_id] = data_request

        handler._close_requests([corr_id])

        assert await data_request._msg_queue.get() is None
        assert not handler._current_requests

    async def test__get_service(self,
                                session_options,
                                open_service_event,
                                ):
        """
        Check that `get_service()` waits for the correct event
        """
        handler = HandlerBase(session_options)
        task = asyncio.create_task(handler._get_service('//blp/refdata'))
        await asyncio.sleep(0.00001)

        handler._session.send_event(open_service_event)
        assert await task
        assert handler._services['//blp/refdata'].is_set()

    async def test___session_handler__start_session__single_thread(
            self,
            session_options,
            open_session_event, ):
        """
        Check that receiving `SessionStarted` blpapi event correctly sets the
        `HandlerRef.session_started` asyncio event
        """
        handler = HandlerBase(session_options)
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
        handler = HandlerBase(session_options)
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
        handler = HandlerBase(session_options)
        assert not handler.session_stopped.is_set()

        # in blpapi this happens under the hood
        handler._session_handler(stop_session_event)

        await handler.session_stopped.wait()
        assert handler.session_stopped.is_set()

    async def test___session_handler__session_startup_failure(
            self,
            session_options,
            session_failure_event):
        """
        Check that receiving `SessionStopped` blpapi event correctly sets the
        `HandlerRef.session_stopped` asyncio event
        """
        handler = HandlerBase(session_options)

        with pytest.raises(BloombergException):
            handler._session_handler(session_failure_event)

    async def test__service_handler__open_service__single_thread(
            self,
            session_options,
            open_service_event):
        """
        Check that receiving `ServiceOpened` blpapi event correctly sets the
        corresponding asyncio event
        """
        handler = HandlerBase(session_options)
        handler._services['//blp/refdata'] = asyncio.Event()

        handler._service_handler(open_service_event)

        await handler._services['//blp/refdata'].wait()
        assert handler._services['//blp/refdata'].is_set()

    async def test__service_handler____open_service__multi_thread(
            self,
            session_options,
            open_service_event):
        """
        Check that receiving `ServiceOpened` blpapi event correctly sets the
        corresponding asyncio event when handler is called from
        a different thread
        """
        handler = HandlerBase(session_options)
        handler._services['//blp/refdata'] = asyncio.Event()

        handler._session.send_event(open_service_event)

        await handler._services['//blp/refdata'].wait()
        assert handler._services['//blp/refdata'].is_set()

    async def test__service_handler__service_opened_failure(
            self,
            session_options,
            service_opened_failure_event):
        handler = HandlerBase(session_options)

        with pytest.raises(BloombergException):
            handler._service_handler(service_opened_failure_event)
