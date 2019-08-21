import asyncio
import uuid

import pytest

from async_blp import AsyncBloomberg
from async_blp.handlers import RequestHandler
from async_blp.requests import ReferenceDataRequest
from async_blp.utils.env_test import CorrelationId


@pytest.mark.asyncio
@pytest.mark.timeout(5)
class TestAsyncBloomberg:

    async def test___choose_handler__free_handler_available(self,
                                                            session_options):
        """
        When free handler is available it should be chosen
        """
        bloomberg = AsyncBloomberg()
        handler = RequestHandler(session_options)
        bloomberg._request_handlers.append(handler)

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
        bloomberg._request_handlers.append(handler)

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
        bloomberg._request_handlers.append(handler_1)
        bloomberg._request_handlers.append(handler_2)

        chosen_handler = bloomberg._choose_handler()

        assert chosen_handler == handler_1

    def test__init__not_inside_loop(self):
        with pytest.raises(RuntimeError):
            AsyncBloomberg()

    async def test___divide_reference_data_request(self):
        bloomberg = AsyncBloomberg(max_fields_per_request=2,
                                   max_securities_per_request=2)

        securities = ['security_1', 'security_2', 'security_3']
        fields = ['field_1', 'field_2', 'field_3']

        chunks = list(bloomberg._split_requests(securities, fields))

        assert len(chunks) == 4
        assert (['security_1', 'security_2'], ['field_1', 'field_2']) in chunks
        assert (['security_1', 'security_2'], ['field_3']) in chunks
        assert (['security_3'], ['field_1', 'field_2']) in chunks
        assert (['security_3'], ['field_3']) in chunks

    @pytest.mark.skip
    async def test__get_reference_data(self,
                                       one_value_array_field_data,
                                       response_event,
                                       open_session_event,
                                       open_service_event):
        field_name, field_values, security_id = one_value_array_field_data

        bloomberg = AsyncBloomberg(max_sessions=1)

        handler = bloomberg._choose_handler()
        session = handler._session

        session.send_event(open_session_event)
        session.send_event(open_service_event)

        task = asyncio.create_task(bloomberg.get_reference_data([security_id],
                                                                [field_name]))

        session.send_event(response_event)

        print(await task)
