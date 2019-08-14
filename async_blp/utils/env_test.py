"""
Emulate blpapi for tests
please  use

try:
    # pylint: disable=ungrouped-imports
    import blpapi
except ImportError:
    # pylint: disable=ungrouped-imports
    from async_blp import env_test as blpapi
"""
import enum
import queue
import threading
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from async_blp.abs_handler import AbsHandler
from async_blp.utils import log

LOGGER = log.get_logger()


# pylint: disable=invalid-name
# pylint: disable=unused-argument

class Event:
    """
    contains Message and type
    """
    PARTIAL_RESPONSE = 'PARTIAL_RESPONSE'
    RESPONSE = 'RESPONSE'
    OTHER = 'other'
    SESSION_STATUS = 'SESSION_STATUS'
    SERVICE_STATUS = 'SERVICE_STATUS'

    def __iter__(self):
        """
        Iterator over messages contained
        """
        return iter(self.msgs)

    def __init__(self, type_: str, msgs: List['Message']):
        """
        A single event resulting from a subscription or a request

        """
        self._type = type_
        self.msgs = msgs

    def eventType(self):
        """
        blpapi uses method instead of attributes
        """
        return self._type

    def destroy(self):
        """
        Destructor
        """


class CorrelationId:
    """
    A key used to identify individual subscriptions or requests.
    """

    def __init__(self, id_):
        self.id_ = id_

    def value(self):
        """
        blpapi uses methods instead of attributes
        """
        return self.id_

    @staticmethod
    def type():
        """
        - Integer (``type() == CorrelationId.INT_TYPE``
          or ``type() == CorrelationId.AUTOGEN_TYPE``)
        - Object (``type() == CorrelationId.OBJECT_TYPE``)
        - ``None`` (``type() == CorrelationId.UNSET_TYPE``)
        """
        return "AUTOGEN"


class SessionOptions:
    """
    blpapi connection Options
    """

    def setServerHost(self, *args, **kwargs):
        """
        Bloomberg Terminal supports only 127.0.0.1
        """

    def setServerPort(self, *args, **kwargs):
        """
        8194 - default port
        """


class Service:
    """
    Provider services are created to generate API data and must be registered
    before use.
    """

    @staticmethod
    def createRequest(requestName):
        """
        An application must populate the :class:`Request` before issuing it
        using :meth:`Session.sendRequest()`.
        :param requestName:
        :return:
        """
        return Request()

    @staticmethod
    def toString(level=0, spacesPerLevel=4):
        """
        for print
        """
        return "test"


class Request:
    """
    object contains the parameters for a single request
    """

    @staticmethod
    def set(key, value):
        """
        Equivalent to :meth:`asElement().setElement(name, value)
               <Element.setElement>`.
        """

    @staticmethod
    def getElement(*args, **kwargs):
        """
         Element: The content of this :class:`Request`
        """
        return Element()


class Message:
    """
    Contains low-level Bloomberg data
    """

    def __init__(self, name, value,
                 children: Dict[str, 'Element'] = None,
                 correlationId: Optional[CorrelationId] = None):
        self._name = name
        self._children = children or {}
        self._value = value
        self._correlation_ids = [correlationId, ]

    def hasElement(self, name):
        """
        blpiapi interface
        """
        return name in self._children

    def correlationIds(self):
        """
        Each :class:`Message` is
            accompanied by a single :class:`CorrelationId`. When
            ``allowMultipleCorrelatorsPerMsg`` is enabled and more than one
            active subscription would result in the same
        """
        return self._correlation_ids

    def asElement(self):
        """
        blpapi Message must be cast
        """
        return Element(self._name, self._value, self._children)

    def name(self):
        """
        blpapi uses method instead of attributes
        """
        return self._name

    def getElement(self, element_name):
        """
        Equivalent to :meth:`asElement().getElement(name)
        """
        return self._children[element_name]

    def __str__(self):
        return str(self.asElement())

    __repr__ = __str__


class Element:
    """
    - A single value of any data type supported by the Bloomberg API
    - An array of values
    - A sequence or a choice
    """

    def __init__(self,
                 name=None,
                 value=None,
                 children: Union[List['Element'], Dict[str, 'Element']] = None,
                 ):
        """
        Init is only for tests; in real blpapi you receive already created
        elements
        """
        self._name = name
        self._children = children or {}
        self._value = value

    def appendValue(self, *args, **kwargs):
        """
        Equivalent to self.setValue(value, internals.ELEMENT_INDEX_END)
        """

    def get_string(self, offset=0):
        """
        Pretty printing; doesn't exist in blpapi
        """
        offset_str = ' ' * offset

        if not self._children:
            return f'{offset_str}{self._name} = {self._value}'

        if isinstance(self._children, list):
            children_str = '\n'.join(child.get_string(offset + 2)
                                     for child in self._children)
            suffix = '[]'

        elif isinstance(self._children, dict):
            children_str = '\n'.join(f'{child.get_string(offset + 2)}'
                                     for child
                                     in self._children.values())
            suffix = '{}'

        else:
            raise RuntimeError(f'Unknown children type: {type(self._children)}')

        return (f'{offset_str}{self._name}{suffix} = {{\n{children_str}\n'
                f'{offset_str}}}')

    def __str__(self):
        return self.get_string()

    __repr__ = __str__

    def getValue(self):
        """
        Can be element or value
        """
        return self._value

    def elements(self):
        """
        Iterator over contained elements
        """
        if isinstance(self._children, dict):
            return list(self._children.values())
        return self._children

    def values(self):
        """
        Iterator over contained values
        """
        return self.elements()

    def datatype(self):
        """
        The possible types are enumerated in :class:`DataType`.
        """
        # this is not in accordance with Bloomberg
        if self.isArray():
            return DataType.SEQUENCE

        return DataType.STRING

    def isArray(self):
        """
        This element is an array if ``elementDefinition().maxValues()>1``
        """
        # this is not in accordance with Bloomberg
        return bool(self._children)

    def getElementAsString(self, element_name: str) -> str:
        """
        Return this element's sub-element with ``element_name`` as a string
        """
        return self.getElement(element_name).getValue()

    def name(self):
        """
        blpapi use func instead attr
        """
        return self._name

    def getElement(self, element_name: str) -> 'Element':
        """
        Return child element; if you use it in tests, make sure that
        `self.children` is a dict
        """
        if isinstance(self._children, dict):
            return self._children[element_name]

        raise RuntimeError

    def hasElement(self, element_name: str) -> bool:
        """
        Return True if child element with `element_name` exists
        """
        if isinstance(self._children, dict):
            return element_name in self._children

        return False


class Session:
    """
    send events to the handler
    """

    def __init__(self,
                 options: SessionOptions = None,
                 eventHandler=None):

        self.options = options
        self.handler = eventHandler
        self.events = queue.Queue()

    def startAsync(self):
        """
        In real blpapi: start Bloomberg session in a separate thread.
        For tests: do nothing, threads will be created later where necessary
        """

    def _async_start(self, handler: AbsHandler):
        """
        Doesn't exists in blpapi; for testing purposes only
        Send all events from `self.events` to the handler one by one.
        """
        while not self.events.empty():
            event = self.events.get(timeout=1)
            LOGGER.debug('Calling handler with %s', event.eventType())
            handler(event, handler.session)

    def openServiceAsync(self, *args, **kwargs):
        """
        Before you can get a Service you need to open it
        """

    def send_event(self, event_: Event):
        """
        Doesn't exists in blpapi; for testing purposes only
        Create new thread and send provided event to the handler

        Events must be send in the following order:
        1) SESSION_STATUS
        2) SERVICE_STATUS
        3) PARTIAL_RESPONSE
        4) RESPONSE

        """
        self.events.put(event_)
        thread = threading.Thread(target=self._async_start,
                                  args=(self.handler,))
        thread.start()

    def sendRequest(self, request, correlationId: CorrelationId):
        """
        all request immediately put close messages
        (@georgy: could you clarify pls?)
        """
        e = Event(
            type_=Event.RESPONSE,
            msgs=[
                Message(value=0, name='test', correlationId=correlationId),
                Message(value=0, name='test', correlationId=correlationId),
                ])

        self.send_event(e)

    @staticmethod
    def getService(*args, **kwargs):
        """
        Just return new service; real blpapi will raise an exception
        if service was not opened
        """
        return Service()


# real blpapi uses string optimization; for testing purposes just str will do
Name = str


class DataType(enum.Enum):
    """
    Contains the possible data types which are represented in an
    :class:`Element`
    """
    SEQUENCE = 'sequence'
    STRING = 'string'
