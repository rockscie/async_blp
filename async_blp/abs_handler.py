"""
abstract Handler for typing
"""

import abc
import asyncio


class AbsHandler(metaclass=abc.ABCMeta):
    """
    All Handler must have session and __call__
    """

    def __init__(self, loop: asyncio.AbstractEventLoop = None):
        self._session = None
        self.session_started = asyncio.Event()
        self.session_stopped = asyncio.Event()
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError('Please create handler inside asyncio loop'
                               'or explicitly provide one')

    @abc.abstractmethod
    def __call__(self, event, session):
        pass

    @abc.abstractmethod
    def get_current_weight(self):
        """
        score for load balance
        """
