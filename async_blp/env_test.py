"""
Emulate blpapi for test
please  use
try:
    import blpapi
except ImportError:
    from tests import env_test as blpapi
"""

import threading
import time
from typing import Dict
from typing import List

from async_blp.abs_handler import AbsHandler


# pylint: disable=invalid-name

class Session:
    """
    send events to the handler
    """

    def __init__(self,
                 options=None,
                 eventHandler=None):

        self.options = options
        self.handler = eventHandler
        self.events = [
            Event(
                type_=Event.RESPONSE,
                msgs=[
                    Message(value=0, name='test'),
                    Message(value=0, name='test'),
                    ]
                ),
            Event(
                type_=Event.OTHER,
                msgs=[
                    Message(value=0, name='SessionStarted'),
                    Message(value=0, name='ServiceOpened'),
                    ]
                )
            ]

    def startAsync(self):
        """
        Start Bloomberg session in a separate thread
        """
        thread = threading.Thread(target=self._async_start,
                                  args=(self.handler,))
        thread.start()

    def _async_start(self, handler: AbsHandler):
        """
        last event type must be Event.RESPONSE
        """
        while self.events:
            time.sleep(0.01)
            event = self.events.pop()
            print(f'Calling handler with {event}')
            handler(event, handler.session)

    def openServiceAsync(self, *args, **kwargs):
        """
        before you can get Service you need to open it
        """


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

    def createRequest(self, requestName):
        return Request()


class Request:

    def set(self, key, value):
        pass

    def getElement(self):
        return Element()


class Event:
    """
    contains Message and type
    """
    RESPONSE = 'RESPONSE'
    OTHER = "other"

    def __iter__(self):
        return iter(self.msgs)

    def __init__(self, type_: str, msgs: List['Message']):
        self._type = type_
        self.msgs = msgs

    def eventType(self):
        """
        blpapi uses method instead of attributes
        """
        return self._type


class Message:
    """
    Contains low-level Bloomberg data
    """

    def __init__(self, name, value, children: Dict[str, 'Element'] = None):
        self._name = name
        self._children = children or {}
        self._value = value

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
        return self._children[element_name]


class Element:

    def __init__(self, name, value, children: Dict[str, 'Element'] = None):
        self._name = name
        self._children = children or {}
        self._value = value

    def appendValue(self):
        pass

    def getValue(self):
        return self._value

    def elements(self):
        return list(self._children.values())

    def values(self):
        return self.elements()

    def datatype(self):
        pass

    def isArray(self):
        return bool(self._children)

    def getElementAsString(self, element_name):
        return self.getElement(element_name).getValue()

    def name(self):
        return self._name

    def getElement(self, element_name):
        return self._children[element_name]


class Name(str):
    pass


class DataType:
    SEQUENCE = 'sequence'
