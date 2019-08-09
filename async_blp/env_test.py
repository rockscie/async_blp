"""
Emulate blpapi for test
please  use

try:
    # pylint: disable=ungrouped-imports
    import blpapi
except ImportError:
    # pylint: disable=ungrouped-imports
    from async_blp import env_test as blpapi
"""
import queue
import threading
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from async_blp.abs_handler import AbsHandler


# pylint: disable=invalid-name
# pylint: disable=unused-argument

class Event:
    """
    contains Message and type
    """
    PARTIAL_RESPONSE = 'PARTIAL_RESPONSE'
    RESPONSE = 'RESPONSE'
    OTHER = "other"
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
    id for mapping request and response
    A key used to identify individual subscriptions or requests.
    """

    def __init__(self, id_):
        """
        Value is only important
        """
        self.id_ = id_

    def value(self):
        """
        blpapi uses method instead of attributes
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

    def __init__(self, name, value, children: Dict[str, 'Element'] = None,
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
        Crating only in test real blp give you element from request or msg
        """
        self._name = name
        self._children = children or {}
        self._value = value

    def appendValue(self, *args, **kwargs):
        """
        self.setValue(value, internals.ELEMENT_INDEX_END)
        """

    def get_children_str(self):
        """
        for print
        """
        if isinstance(self._children, list) and self._children:
            return '\n\t'.join([str(child)
                                for child in self._children])

        if isinstance(self._children, dict) and self._children:
            return '\n\t'.join([f'{name} = {child.get_children_str()}'
                                for name, child in self._children.items()])

        return f'\t{self._value}'

    def __str__(self):
        if not self._children:
            return f'{self._name} = {self._value}'

        if isinstance(self._children, list):
            suffix = '[]'
        else:
            suffix = '{}'

        return f'{self._name}{suffix} = {{\n {self.get_children_str()} \n  }}'

    __repr__ = __str__

    def getValue(self):
        """
        can be element or value
        """
        return self._value

    def elements(self):
        """
        Iterator over elements contained
        """
        if isinstance(self._children, dict):
            return list(self._children.values())
        return self._children

    def values(self):
        """
        Iterator over values contained
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

    def getElementAsString(self, element_name):
        """
        str: This element's sub-element with ``name`` as a string
        """
        return self.getElement(element_name).getValue()

    def name(self):
        """
        blpapi use func instead attr
        """
        return self._name

    def getElement(self, element_name):
        """
                be careful there no testing
                """
        if isinstance(self._children, dict):
            return self._children[element_name]
        raise RuntimeError


class Session:
    """
    send events to the handler
    """

    def __init__(self,
                 options=None,
                 eventHandler=None):
        """
        for each action we will create new thread
        at init do nothing
        """

        self.options = options
        self.handler = eventHandler
        self.events = queue.Queue()

    def startAsync(self):
        """
        Start Bloomberg session in a separate thread
        """

    def _async_start(self, handler: AbsHandler):
        """
        last event type must be Event.RESPONSE
        """
        while not self.events.empty():
            event = self.events.get(timeout=1)
            print(f'Calling handler with {event.eventType()}')
            handler(event, handler.session)

    def openServiceAsync(self, *args, **kwargs):
        """
        before you can get Service you need to open it
        """

    def send_event(self, event_: Event):
        """
        for testing you must create correct events amd sent it by properly time
        """
        self.events.put(event_)
        thread = threading.Thread(target=self._async_start,
                                  args=(self.handler,))
        thread.start()

    def sendRequest(self, request, correlationId: CorrelationId):
        """
        all request immediately put close messages
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
        blpapi will raise if  Service is not open , test will work
        """
        return Service()


class Name(str):
    """
    is same as str in blpapi there same optimization
    """


# pylint: disable=too-few-public-methods
class DataType:
    """
    Contains the possible data types which can be represented in an
    :class:`Element
    """
    SEQUENCE = 'sequence'
    STRING = 'string'
