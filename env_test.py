import threading
import time
from typing import List


class Msg:

    def __init__(self, value, name):
        self.value = value
        self._name = name

    def asElement(self):
        return self

    def name(self):
        return self._name


class Session:

    def __init__(self, options, eventHandler):
        self.handler = eventHandler
        self.events = [
            Event(
                type=Event.RESPONSE,
                msgs=[
                    Msg(value=0, name='test'),
                    Msg(value=0, name='test'),
                    ]
                ),
            Event(
                type=Event.OTHER,
                msgs=[
                    Msg(value=0, name='SessionStarted'),
                    Msg(value=0, name='ServiceOpened'),
                    ]
                )
            ]

    def startAsync(self):
        thread = threading.Thread(target=self._async_start,
                                  args=(self.handler,))
        thread.start()

    # todo add abc handler
    def _async_start(self, handler):
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

    def __init__(self, type: str, msgs: List[Msg]):
        self._type = type
        self.msgs = msgs

    def eventType(self):
        return self._type
