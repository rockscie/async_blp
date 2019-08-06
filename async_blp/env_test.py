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
from typing import List

from async_blp.abs_handler import AbcHandler


class Message:
    """
    contain low level Bloomberg data
    """

    def __init__(self, value, name):
        self.value = value
        self._name = name

    # pylint: disable=invalid-name
    def asElement(self):
        """
        blpapi Message must be cast
        """
        return self

    def name(self):
        """
        blpapi use method instead attr
        """
        return self._name


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

    # pylint: disable=invalid-name
    def startAsync(self):
        """
        async Bloomberg use thead
        """
        thread = threading.Thread(target=self._async_start,
                                  args=(self.handler,))
        thread.start()

    def _async_start(self, handler: AbcHandler):
        """
        to correct work las event type must  Event.RESPONSE
        """
        while self.events:
            time.sleep(0.01)
            event = self.events.pop()
            print(f'Calling handler with {event}')
            handler(event, handler.session)

    def openServiceAsync(self, *args, **kwargs):
        """
        before you can get  Service you need open it
        """


class SessionOptions:
    """
    blpapi connecting Options
    """

    # pylint: disable=invalid-name
    def setServerHost(self, *args, **kwargs):
        """
        for terminal 127.0.0.1 only
        """

    # pylint: disable=invalid-name
    def setServerPort(self, *args, **kwargs):
        """
        8194 - standard
        """


class Event:
    """
    contains Message and type
    """
    RESPONSE = 'RESPONSE'
    OTHER = "other"

    def __iter__(self):
        return iter(self.msgs)

    def __init__(self, type_: str, msgs: List[Message]):
        self._type = type_
        self.msgs = msgs

    # pylint: disable=invalid-name
    def eventType(self):
        """
        blpapi use method instead attr
        """
        return self._type
