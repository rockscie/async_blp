import abc
import asyncio
from typing import Any
from typing import Dict
from typing import Optional

from async_blp.enums import ErrorBehaviour
from async_blp.utils import log

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

LOGGER = log.get_logger()


class RequestBase(metaclass=abc.ABCMeta):
    service_name = None
    request_name = None

    def __init__(self,
                 request_options: Dict[str, Any],
                 error_behavior: ErrorBehaviour = ErrorBehaviour.RETURN,
                 loop: asyncio.AbstractEventLoop = None,
                 ):
        try:
            self._loop = loop or asyncio.get_running_loop()
            self._msg_queue = asyncio.Queue(loop=self._loop)
        except RuntimeError:
            self._loop = None
            self._msg_queue: Optional[asyncio.Queue] = None

        self._error_behaviour = error_behavior
        self._request_options = request_options or {}

    def send_queue_message(self, msg):
        """
        Thread-safe method that put the given msg into async queue
        """
        if self._loop is None or self._msg_queue is None:
            raise RuntimeError('Please create request inside async loop or set '
                               'loop explicitly if you want to use async')

        self._loop.call_soon_threadsafe(self._msg_queue.put_nowait, msg)
        LOGGER.debug('%s: message sent', self.__class__.__name__)

    async def _get_message_from_queue(self):
        LOGGER.debug('%s: waiting for messages', self.__class__.__name__)
        msg: blpapi.Message = await self._msg_queue.get()

        if msg is None:
            LOGGER.debug('%s: last message received, processing is '
                         'finished',
                         self.__class__.__name__)

        LOGGER.debug('%s: message received', self.__class__.__name__)

        return msg

    def set_running_loop_as_default(self):
        """
        Set currently active loop as default for this request and create
        new message queue
        """
        self._loop = asyncio.get_running_loop()

        if self._msg_queue is not None and not self._msg_queue.empty():
            raise RuntimeError('Current message queue is not empty')

        self._msg_queue = asyncio.Queue()
        LOGGER.debug('%s: loop has been changed', self.__class__.__name__)

    def create(self, service: blpapi.Service) -> blpapi.Request:
        """
        Create Bloomberg request. Given `service` must be opened beforehand.
        """
        request = service.createRequest(self.request_name)

        for name, value in self._request_options.items():
            if isinstance(value, list):
                for item in value:
                    request.append(name, item)

            else:
                request.set(name, value)

        return request

    @abc.abstractmethod
    async def process(self):
        pass

    @property
    @abc.abstractmethod
    def weight(self) -> int:
        """
        Approximate request complexity; used to balance load
        between handlers. More complex requests receive higher value.
        """
        pass
