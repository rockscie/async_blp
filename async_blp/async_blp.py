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

from async_blp.requests import HistoricalDataRequest
from async_blp.utils.misc import split_into_chunks
from .enums import ErrorBehaviour
from .enums import SecurityIdType
from .handler_refdata import RequestHandler
from .requests import ReferenceDataRequest
from .utils import log

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

        self._handlers: List[RequestHandler] = []

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
        for handler in self._handlers:
            handler.stop_session()

        all_events = [handler.session_stopped.wait()
                      for handler in self._handlers]

        await asyncio.gather(*all_events)

    async def get_reference_data(
            self,
            securities: List[str],
            fields: List[str],
            security_id_type: Optional[SecurityIdType] = None,
            overrides=None,
            ) -> Tuple[pd.DataFrame, Dict]:
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
        errors = {}

        for data, error in requests_result:
            result_df.loc[data.index, data.columns] = data
            errors.update(error)

        return result_df, errors

    async def get_historical_data(
            self,
            securities: List[str],
            fields: List[str],
            start_date: dt.date,
            end_date: dt.date,
            security_id_type: Optional[SecurityIdType] = None,
            overrides=None,
            ) -> Tuple[pd.DataFrame, Dict]:
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
        result_df = pd.DataFrame(index=securities, columns=fields)
        errors = {}

        for data, error in requests_result:
            result_df.loc[data.index, data.columns] = data
            errors.update(error)

        multi_index = pd.MultiIndex.from_arrays([result_df.index,
                                                 result_df['Date']])

        result_df.index = multi_index

        return result_df, errors

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
                         for handler in self._handlers
                         if not handler.get_current_weight()]

        if free_handlers:
            return free_handlers[0]

        if len(self._handlers) < self._max_sessions:
            handler = RequestHandler(self._session_options, self._loop)
            self._handlers.append(handler)
            return handler

        return min([handler for handler in self._handlers],
                   key=lambda handler: handler.get_current_weight())

    def _split_requests(self,
                        securities: List[str],
                        fields: List[str]):

        securities_chunks = split_into_chunks(securities,
                                              self._max_securities_per_request)

        fields_chunks = split_into_chunks(fields,
                                          self._max_fields_per_request)

        return product(securities_chunks, fields_chunks)
