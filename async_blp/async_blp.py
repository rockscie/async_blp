"""
high level Api
"""
import asyncio
import datetime as dt
import logging
from itertools import product
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd

from .enums import ErrorBehaviour
from .enums import SecurityIdType
from .errors import BloombergErrors
from .handlers import RequestHandler
from .handlers import SubscriptionHandler
from .instruments_requests import CurveLookupRequest
from .instruments_requests import GovernmentLookupRequest
from .instruments_requests import SecurityLookupRequest
from .requests import HistoricalDataRequest
from .requests import ReferenceDataRequest
from .requests import SearchField
from .requests import Subscription
from .utils import log
from .utils.misc import split_into_chunks

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

LOGGER = log.get_logger()


class AsyncBloomberg:
    """
    Async wrapper of blpapi
    """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 host: str = '127.0.0.1',
                 port: int = 8194,
                 log_level: int = logging.WARNING,
                 loop: asyncio.AbstractEventLoop = None,
                 error_behaviour: ErrorBehaviour = ErrorBehaviour.IGNORE,
                 max_sessions: int = 5,
                 max_securities_per_request: int = 100,
                 max_fields_per_request: int = 50,
                 ):
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError('Please run AsyncBloomberg inside asyncio '
                               'loop or explicitly provide one')

        self._max_fields_per_request = max_fields_per_request
        self._max_securities_per_request = max_securities_per_request
        self._max_sessions = max_sessions
        self._error_behaviour = error_behaviour

        self._session_options = blpapi.SessionOptions()
        self._session_options.setServerHost(host)
        self._session_options.setServerPort(port)

        self._request_handlers: List[RequestHandler] = []
        self._subscription_handler: Optional[SubscriptionHandler] = None

        log.set_logger(log_level)

    async def stop(self):
        """
        Stop all started sessions. If you try to use `AsyncBloomberg` after
        calling this method, it will attempt to open new sessions.

        If you stop session before receiving full response from Bloomberg,
        you may lose some of the data.

        This method waits for all handlers to successfully
        stop their sessions.
        """
        for handler in self._request_handlers:
            handler.stop_session()

        if self._subscription_handler:
            self._subscription_handler.stop_session()
            all_events = [self._subscription_handler.session_stopped.wait()]
        else:  # pragma: no cover
            all_events = []

        all_events.extend(handler.session_stopped.wait()
                          for handler in self._request_handlers)

        await asyncio.gather(*all_events)

    async def get_reference_data(
            self,
            securities: List[str],
            fields: List[str],
            security_id_type: Optional[SecurityIdType] = None,
            overrides=None,
            ) -> Tuple[pd.DataFrame, BloombergErrors]:
        """
        Return reference data from Bloomberg
        """
        chunks = self._split_requests(securities, fields)
        request_tasks = []

        for security_chunk, fields_chunk in chunks:
            handler = self._choose_handler()

            request = ReferenceDataRequest(security_chunk,
                                           fields_chunk,
                                           security_id_type,
                                           overrides,
                                           self._error_behaviour,
                                           self._loop)

            request_tasks.append(asyncio.create_task(request.process()))
            asyncio.create_task(handler.send_requests([request]))

        requests_result = await asyncio.gather(*request_tasks)
        result_df = pd.DataFrame(index=securities, columns=fields)
        errors = BloombergErrors()

        for data, error in requests_result:
            result_df.loc[data.index, data.columns] = data
            errors += error

        return result_df, errors

    async def search_fields(self,
                            query: str,
                            overrides=None,
                            ) -> pd.DataFrame:
        """
        Return reference data from Bloomberg
        """

        request = SearchField(query,
                              overrides,
                              self._error_behaviour,
                              self._loop)
        handler = self._choose_handler()

        asyncio.create_task(handler.send_requests([request]))

        requests_result = await request.process()

        data = requests_result

        return data

    async def get_historical_data(
            self,
            securities: List[str],
            fields: List[str],
            start_date: dt.date,
            end_date: dt.date,
            security_id_type: Optional[SecurityIdType] = None,
            overrides=None,
            ) -> Tuple[pd.DataFrame, BloombergErrors]:
        """
        Return historical data from Bloomberg
        """

        chunks = self._split_requests(securities, fields)
        tasks = []

        for security_chunk, fields_chunk in chunks:
            handler = self._choose_handler()

            request = HistoricalDataRequest(security_chunk,
                                            fields_chunk,
                                            start_date,
                                            end_date,
                                            security_id_type,
                                            overrides,
                                            self._error_behaviour,
                                            self._loop)

            tasks.append(asyncio.create_task(request.process()))
            asyncio.create_task(handler.send_requests([request]))

        requests_result = await asyncio.gather(*tasks)

        all_dates = pd.date_range(start_date, end_date)
        index = pd.MultiIndex.from_product([all_dates, securities],
                                           names=['date', 'security'])

        result_df = pd.DataFrame(index=index,
                                 columns=fields)
        errors = BloombergErrors()

        for data, error in requests_result:
            result_df.loc[data.index, data.columns] = data
            errors += BloombergErrors()

        return result_df, errors

    async def add_subscriber(
            self,
            securities: List[str],
            fields: List[str],
            security_id_type: Optional[SecurityIdType] = None,
            overrides=None,
            ) -> None:
        """
        all subscribe in one session
        """

        request = Subscription(securities,
                               fields,
                               security_id_type,
                               overrides,
                               self._error_behaviour,
                               self._loop)

        if self._subscription_handler is None:
            self._subscription_handler = SubscriptionHandler(
                self._session_options,
                self._loop)

        await self._subscription_handler.subscribe([request])

    async def read_subscriber(self) -> pd.DataFrame:
        """
        all subscribe in one session
        """
        if self._subscription_handler is None:
            raise RuntimeError('You have to subscribe before reading '
                               'subscription data')

        return await self._subscription_handler.read_subscribers()

    async def security_lookup(self,
                              query: str,
                              options: Dict[str, str] = None,
                              max_results: int = 10):
        options = options or {}
        handler = self._choose_handler()

        request = SecurityLookupRequest(query, max_results, options,
                                        self._error_behaviour, self._loop)

        task = asyncio.create_task(request.process())
        asyncio.create_task(handler.send_requests([request]))

        return await task

    async def curve_lookup(self,
                           query: str,
                           options: Dict[str, str] = None,
                           max_results: int = 10):
        options = options or {}
        handler = self._choose_handler()

        request = CurveLookupRequest(query, max_results, options,
                                     self._error_behaviour, self._loop)

        task = asyncio.create_task(request.process())
        asyncio.create_task(handler.send_requests([request]))

        return await task

    async def government_lookup(self,
                                query: str,
                                options: Dict[str, str] = None,
                                max_results: int = 10):
        options = options or {}
        handler = self._choose_handler()

        request = GovernmentLookupRequest(query, max_results, options,
                                          self._error_behaviour, self._loop)

        task = asyncio.create_task(request.process())
        asyncio.create_task(handler.send_requests([request]))

        return await task

    def _choose_handler(self) -> RequestHandler:
        """
        Return the most suitable handler to handle new request using
        the following rules:
            1) If there are free handlers (with no current requests),
               return one of them
            2) If new handler can be created (`max_sessions` is not reached),
               return new handler
            3) Otherwise, return the handler with the smallest load

        """
        free_handlers = [handler
                         for handler in self._request_handlers
                         if not handler.current_load]

        if free_handlers:
            return free_handlers[0]

        if len(self._request_handlers) < self._max_sessions:
            handler = RequestHandler(self._session_options, self._loop)
            self._request_handlers.append(handler)
            return handler

        return min([handler for handler in self._request_handlers],
                   key=lambda handler: handler.current_load)

    def _split_requests(self,
                        securities: List[str],
                        fields: List[str]):

        securities_chunks = split_into_chunks(securities,
                                              self._max_securities_per_request)

        fields_chunks = split_into_chunks(fields,
                                          self._max_fields_per_request)

        return product(securities_chunks, fields_chunks)
