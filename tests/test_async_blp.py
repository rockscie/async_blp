import asyncio
import datetime as dt
import uuid
from typing import Callable

import pandas as pd
import pytest

from async_blp import AsyncBloomberg
from async_blp.errors import BloombergErrors
from async_blp.handlers import RequestHandler
from async_blp.requests import ReferenceDataRequest
from async_blp.utils.blp_name import SECURITY_DATA
from async_blp.utils.env_test import CorrelationId
from async_blp.utils.env_test import Event
from async_blp.utils.env_test import Message


@pytest.mark.asyncio
@pytest.mark.timeout(10)
class TestAsyncBloomberg:

    async def _create_task(self,
                           open_session_event: Event,
                           open_service_event: Event,
                           event: Event,
                           l_func: Callable):
        """
        create AsyncBloomberg and send l_func
        look on all preparation will be complete for get result
        """
        bloomberg = AsyncBloomberg(max_sessions=1)
        loop = asyncio.get_running_loop()
        task = loop.create_task(l_func(bloomberg))
        handler = bloomberg._choose_handler()
        session = handler._session

        session.send_event(open_session_event)
        session.send_event(open_service_event)
        await asyncio.sleep(0.0001)
        self.put_id_in_handler(event, handler)
        session.send_event(event)
        response = await task
        return response

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

    @staticmethod
    def put_id_in_handler(response_event, handler):
        msg = list(response_event.msgs)[0]
        cor_id = msg.correlationIds()[0]
        req = list(handler._current_requests.values())[0]
        handler._current_requests = {cor_id: req}

    async def test__get_reference_data(self,
                                       one_value_array_field_data,
                                       response_event,
                                       open_session_event,
                                       open_service_event):
        field_name, field_values, security_id = one_value_array_field_data

        def ref_send(bloomberg):
            return bloomberg.get_reference_data([security_id], [field_name])

        data, errors = await self._create_task(open_session_event,
                                               open_service_event,
                                               response_event,
                                               ref_send)
        assert errors == BloombergErrors()

        expected_data = pd.DataFrame([[field_values]],
                                     index=[security_id],
                                     columns=[field_name],
                                     )

        pd.testing.assert_frame_equal(expected_data, data)

    async def test__get_historical_data(self,
                                        security_data_historical,
                                        simple_field_data,
                                        open_session_event,
                                        open_service_event):
        field_name, field_value, security_id = simple_field_data

        response_event = Event('RESPONSE', [
            Message('Response', None, {
                SECURITY_DATA: security_data_historical,
                })
            ])

        def hist_send(bloomberg):
            return bloomberg.get_historical_data(
                [security_id],
                [field_name],
                dt.date(2018, 1, 1),
                dt.date(2018, 1, 5))

        data, errors = await self._create_task(open_session_event,
                                               open_service_event,
                                               response_event,
                                               hist_send)

        index = pd.MultiIndex.from_tuples([
            (pd.Timestamp(dt.date(2018, 1, 1)), security_id),
            (pd.Timestamp(dt.date(2018, 1, 2)), security_id),
            (pd.Timestamp(dt.date(2018, 1, 3)), security_id),
            (pd.Timestamp(dt.date(2018, 1, 4)), security_id),
            (pd.Timestamp(dt.date(2018, 1, 5)), security_id),
            ],
            names=['date', 'security'])

        expected_data = pd.DataFrame([field_value, None, None, None, None],
                                     index=index,
                                     columns=[field_name],
                                     dtype=object)

        assert errors == BloombergErrors()

        pd.testing.assert_frame_equal(expected_data, data)

    async def test__search_fields(self, field_search_msg, open_session_event,
                                  open_service_event):
        event = Event('RESPONSE', [field_search_msg])

        def search_send(bloomberg):
            return bloomberg.search_fields('Price')

        data = await self._create_task(open_session_event,
                                       open_service_event,
                                       event,
                                       search_send)

        expected_data = pd.DataFrame([['Theta Last Price', 'THETA_LAST',
                                       'Double']],
                                     index=['OP179'],
                                     columns=['description', 'mnemonic',
                                              'datatype'])

        pd.testing.assert_frame_equal(expected_data, data)

    async def test__security_lookup(self, open_service_event,
                                    open_session_event, security_lookup_msg):
        event = Event('RESPONSE', [security_lookup_msg])

        def lookup_send(bloomberg):
            return bloomberg.security_lookup('Ford')

        data, _ = await self._create_task(open_session_event,
                                          open_service_event,
                                          event,
                                          lookup_send)

        expected_data = pd.DataFrame([['F US Equity', 'Ford Motors Co']],
                                     columns=['security', 'description'])

        pd.testing.assert_frame_equal(expected_data, data)

    async def test__curve_lookup(self, open_service_event,
                                 open_session_event, curve_lookup_msg):
        event = Event('RESPONSE', [curve_lookup_msg])

        def curve_send(bloomberg: AsyncBloomberg):
            return bloomberg.curve_lookup('Ford')

        data, _ = await self._create_task(open_session_event,
                                          open_service_event,
                                          event,
                                          curve_send)

        expected_data = pd.DataFrame([['GOLD',
                                       'US',
                                       'USD',
                                       'CD1016',
                                       'CORP',
                                       'CDS',
                                       'Bloomberg',
                                       'YCCD1016',
                                       ]],
                                     columns=['description',
                                              'country',
                                              'currency',
                                              'curveid',
                                              'type',
                                              'subtype',
                                              'publisher',
                                              'bbgid',
                                              ])

        pd.testing.assert_frame_equal(expected_data, data)

    async def test__government_lookup(self, open_service_event,
                                      open_session_event,
                                      government_lookup_msg):
        event = Event('RESPONSE', [government_lookup_msg])

        def gov_send(bloomberg: AsyncBloomberg):
            return bloomberg.government_lookup('Ford')

        data, _ = await self._create_task(open_session_event,
                                          open_service_event,
                                          event,
                                          gov_send)

        expected_data = pd.DataFrame([['T',
                                       'Treasuries',
                                       'T',
                                       ]],
                                     columns=['parseky',
                                              'name',
                                              'ticker',
                                              ])

        pd.testing.assert_frame_equal(expected_data, data)
