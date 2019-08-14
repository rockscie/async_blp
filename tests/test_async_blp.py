import uuid

import pytest

from async_blp import AsyncBloomberg
from async_blp.handler_refdata import RequestHandler
from async_blp.requests import ReferenceDataRequest
from async_blp.utils.env_test import CorrelationId


@pytest.mark.asyncio
class TestAsyncBloomberg:

    async def test___choose_handler__free_handler_available(self,
                                                            session_options):
        """
        When free handler is available it should be chosen
        """
        bloomberg = AsyncBloomberg()
        handler = RequestHandler(session_options)
        bloomberg._handlers.append(handler)

        chosen_handler = bloomberg._choose_handler()

        assert chosen_handler == handler

    async def test___chose_handler__free_slot_available(self,
                                                        session_options):
        """
        When there are no free handlers and `max_sessions` is not reached,
        new handler should be created
        """
        bloomberg = AsyncBloomberg()
        handler = RequestHandler(session_options)
        request = ReferenceDataRequest(['security_id'], ['field'])
        corr_id = CorrelationId(uuid.uuid4())

        handler._current_requests[corr_id] = request
        bloomberg._handlers.append(handler)

        chosen_handler = bloomberg._choose_handler()

        assert chosen_handler != handler

    async def test___chose_handler__max_sessions_reached(self,
                                                         session_options):
        """
        When there are no free handlers and `max_sessions` is reached,
        the least busy handler should be chosen
        """
        bloomberg = AsyncBloomberg(max_sessions=2)
        handler_1 = RequestHandler(session_options)
        handler_2 = RequestHandler(session_options)

        request_1 = ReferenceDataRequest(['security_id'], ['field'])
        request_2 = ReferenceDataRequest(['security_id', 'security_id_2'],
                                         ['field', 'field_2'])

        corr_id_1 = CorrelationId(uuid.uuid4())
        corr_id_2 = CorrelationId(uuid.uuid4())

        handler_1._current_requests[corr_id_1] = request_1
        handler_2._current_requests[corr_id_2] = request_2
        bloomberg._handlers.append(handler_1)
        bloomberg._handlers.append(handler_2)

        chosen_handler = bloomberg._choose_handler()

        assert chosen_handler == handler_1
