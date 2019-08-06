import asyncio
from typing import List

try:
    import blpapi
except:
    from tests import env_test as blpapi


class HandlerRef:
    connection = False
    service_name = "//blp/refdata"
    request_name = "ReferenceDataRequest"

    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.event = asyncio.Event()
        session_options = blpapi.SessionOptions()
        session_options.setServerHost("localhost")
        session_options.setServerPort(8194)
        self.__result = []

        self.session = blpapi.Session(options=session_options,
                                      eventHandler=self)
        self.session.startAsync()

    def send_requests(self, requests: List):
        self.requests = requests
        self.event.clear()

    def _send_requests(self):
        if self.connection:
            raise ValueError("can't send request until bloom is not started")
        service = self.session.getService(self.service_name)
        for request_obj in self.requests:
            request = service.createRequest(self.request_name)
            request.getElement("securities").appendValue(request_obj.isin)
            request.getElement("fields").appendValue(request_obj.field)
            self.session.sendRequest(request)

    def __call__(self, event: blpapi.Event, session: blpapi.Session):
        print('got type ', event.eventType())
        for msg in event:
            self.__result.append(msg)
            if msg.asElement().name() == 'SessionStarted':
                session.openServiceAsync(self.service_name)
            if msg.asElement().name() == 'ServiceOpened':
                self.connection = True
            if event.eventType() == blpapi.Event.RESPONSE:
                self.loop.call_soon_threadsafe(set_event, self.event)


def set_event(event):
    event.set()
