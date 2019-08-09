"""
all fixture is samples of blpapi events

this events must be sent in session in property time

"""

import pytest

from async_blp.env_test import Element
from async_blp.env_test import Event
from async_blp.env_test import Message
from async_blp.env_test import SessionOptions
from async_blp.requests import ReferenceDataRequest


# pylint is not like pytest.fixture but we do
# pylint: disable=redefined-outer-name

@pytest.fixture()
def session_options() -> SessionOptions():
    """
    for test not important
    """
    session_options_ = SessionOptions()
    session_options_.setServerHost("localhost")
    session_options_.setServerPort(8194)
    return session_options_


@pytest.fixture()
def data_request():
    """
    Simple request
    """
    field_name = 'PX_LAST'
    security_id = 'F Equity'
    return ReferenceDataRequest([security_id], [field_name])


@pytest.fixture()
def open_session_event():
    """
    first event
    """
    event_ = Event(type_=Event.SESSION_STATUS,
                   msgs=[Message(value=0, name='SessionStarted'), ]
                   )
    return event_


@pytest.fixture()
def open_service_event():
    """
    first event
    """
    event_ = Event(type_=Event.SERVICE_STATUS,
                   msgs=[Message(value=0, name='ServiceOpened'), ]
                   )
    return event_


@pytest.fixture()
def element_daily_reached():
    """
    error when you load to many
    """
    return Element(name='subcategory', value='DAILY_LIMIT_REACHED')


@pytest.fixture()
def msg_daily_reached(element_daily_reached):
    """
    error when you load to many
    """
    return Message(name="responseError",
                   value='',
                   children={
                       "responseError": element_daily_reached
                       }
                   )


@pytest.fixture()
def element_monthly_reached():
    """
    error when you load to many
    """
    return Element(name='subcategory', value='MONTHLY_LIMIT_REACHED')


@pytest.fixture()
def error_event(msg_daily_reached):
    """
    error when you load to many
    """
    return Event(type_=Event.RESPONSE,
                 msgs=[
                     msg_daily_reached,
                     ],
                 )
