"""
high level Api
"""
import asyncio
import logging
from typing import List
from typing import Optional

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
                 max_sessions: int = 1,
                 error_behaviour: ErrorBehaviour = ErrorBehaviour.IGNORE,
                 ):
        self._loop = loop or asyncio.get_running_loop()
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
            ):
        """
        Return reference data from Bloomberg
        """

        if self._handlers:
            handler = self._handlers[0]
        else:
            handler = RequestHandler(self._session_options)
            self._handlers.append(handler)

        request = ReferenceDataRequest(securities, fields, security_id_type,
                                       overrides)

        asyncio.create_task(handler.send_requests([request]))
        data, errors = await request.process()

        return data, errors
