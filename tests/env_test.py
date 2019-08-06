"""
Emulate blpapi for test
please  use
try:
    import blpapi
except:
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

    def asElement(self):
        """
        blpapi Message must be cast
        """
        return self

    def name(self):
        return self._name


class Session:
    """
    send events to the handler
    """

    def __init__(self,
                 options=None,
                 eventHandler=None):

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
        pass


class SessionOptions:

    def setServerHost(self, *args, **kwargs):
        pass

    def setServerPort(self, *args, **kwargs):
        pass


class Event:
    RESPONSE = 'RESPONSE'
    OTHER = "other"

    def __iter__(self):
        return iter(self.msgs)

    def __init__(self, type_: str, msgs: List[Message]):
        self._type = type_
        self.msgs = msgs

    def eventType(self):
        return self._type
