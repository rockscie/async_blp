"""
For test Handler we must use env_test for emulate Bloomberg
"""
import asyncio

import pytest

from async_blp.handler_refdata import HandlerRef


# pylint is not like pytest.fixture but we do
# pylint: disable=redefined-outer-name
# it's test we need protected-access
# pylint: disable=protected-access


@pytest.mark.asyncio
@pytest.mark.timeout(11)
class TestHandleRef:
    """
    test all async methods
    """

    async def test_start_connection(self,
                                    session_options,
                                    open_session_event,
                                    ):
        """
        we try connect in init
        """
        handler = HandlerRef(session_options)
        assert not handler.connection.is_set()
        handler._session_handler(open_session_event)
        await handler.connection.wait()
        assert handler.connection.is_set()

        handler.connection.clear()
        handler.session.send_event(open_session_event)
        await handler.connection.wait()
        assert handler.connection.is_set()

    async def test_get_service(self,
                               session_options,
                               open_service_event,
                               ):
        """
        only handler knows when we can open Service
        """
        handler = HandlerRef(session_options)
        task = asyncio.create_task(handler._get_service('test'))
        await asyncio.sleep(0.00001)
        handler.session.send_event(open_service_event)
        assert await task
        assert handler.services['test'].is_set()

        handler.services['test'].clear()
        handler._service_handler(open_service_event)
        await handler.services['test'].wait()
        assert handler.services['test'].is_set()

    async def test_send_requests_id(self,
                                    session_options,
                                    data_request,
                                    ):
        """
        we create correlationId for each requests
        """
        handler = HandlerRef(session_options)
        handler.connection.set()
        task = asyncio.create_task(handler.send_requests([data_request]))
        task1 = asyncio.create_task(handler.send_requests([data_request]))
        data_request.loop = asyncio.get_running_loop()
        await asyncio.sleep(0.00001)
        assert len(handler.requests) > 1, "all requests must have own id"
        task.cancel()
        task1.cancel()

    async def test_call_limit(self, session_options,
                              data_request,
                              msg_daily_reached, ):
        """
        only handler knows when we can open Service
        """
        handler = HandlerRef(session_options)
        data_request.loop = asyncio.get_running_loop()
        handler._close_requests([data_request])
        assert await data_request.msg_queue.get() is None

        handler.requests[None] = data_request
        handler._is_error_msg(msg_daily_reached)
        assert await data_request.msg_queue.get() is None

    async def test_star_stop(self,
                             session_options,
                             data_request):
        """
        Just open service and wait for RESPONSE
        """

        data_request.loop = asyncio.get_running_loop()
        handler = HandlerRef(session_options)
        asyncio.create_task(handler.send_requests([data_request]))
        asyncio.create_task(data_request.process())
        await data_request.process()
        assert True
