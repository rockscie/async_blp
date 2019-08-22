"""
For testing purposes, please use `Session.send_event` method that emulates
blpapi and opens another thread. You can also call the needed handler method
directly.
"""
import datetime as dt
import logging

import pytest

from async_blp.requests import ReferenceDataRequest
from async_blp.utils import log
from async_blp.utils.blp_name import ERROR_INFO
from async_blp.utils.blp_name import FIELD_DATA
from async_blp.utils.blp_name import FIELD_EXCEPTIONS
from async_blp.utils.blp_name import FIELD_ID
from async_blp.utils.blp_name import MESSAGE
from async_blp.utils.blp_name import SECURITY
from async_blp.utils.blp_name import SECURITY_DATA
from async_blp.utils.blp_name import SECURITY_ERROR
from async_blp.utils.env_test import CorrelationId
from async_blp.utils.env_test import Element
from async_blp.utils.env_test import Event
from async_blp.utils.env_test import Message
from async_blp.utils.env_test import SessionOptions


# pylint does not like pytest.fixture but we do
# pylint: disable=redefined-outer-name


@pytest.fixture(autouse=True, scope='function')
def debug_logs():
    """
    Show all logs for tests
    """
    log.set_logger(logging.DEBUG)
    yield
    log.set_logger(logging.DEBUG)


@pytest.fixture()
def session_options() -> SessionOptions():
    """
    For tests it's not important
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
    SessionStarted event that is the first event that Bloomberg sends,
    indicates that session was successfully opened
    """
    event_ = Event(type_=Event.SESSION_STATUS,
                   msgs=[Message(value=0, name='SessionStarted'), ]
                   )
    return event_


@pytest.fixture()
def stop_session_event():
    """
    SessionStopped event that is the very last event that Bloomberg sends,
    after user calls `session.stopAsync`
    """
    event_ = Event(type_=Event.SESSION_STATUS,
                   msgs=[Message(value=0, name='SessionTerminated',
                                 ),
                         ]
                   )
    return event_


@pytest.fixture()
def session_failure_event():
    """
    SessionStopped event that is the very last event that Bloomberg sends,
    after user calls `session.stopAsync`
    """
    event_ = Event(type_=Event.SESSION_STATUS,
                   msgs=[Message(value=0,
                                 name='SessionStartupFailure',
                                 ),
                         ]
                   )
    return event_


@pytest.fixture()
def open_service_event():
    """
    ServiceOpened event, indicates that service was successfully opened
    """
    msg = Message(value=0,
                  name='ServiceOpened',
                  children={
                      'serviceName': Element(value="//blp/refdata")
                      }
                  )

    event_ = Event(type_=Event.SERVICE_STATUS, msgs=[msg])
    return event_


@pytest.fixture()
def service_opened_failure_event():
    """
    ServiceOpened event, indicates that service was successfully opened
    """
    msg = Message(value=0,
                  name='ServiceOpenedFailure',
                  children={
                      'serviceName': Element(value="//blp/refdata")
                      }
                  )

    event_ = Event(type_=Event.SERVICE_STATUS, msgs=[msg])
    return event_


@pytest.fixture()
def element_daily_reached():
    """
    Error indicating that too many requests were made
    """
    return Element(name='subcategory', value='DAILY_LIMIT_REACHED')


@pytest.fixture()
def msg_daily_reached(element_daily_reached):
    """
    Error indicating that too many requests were made
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
    Error indicating that too many requests were made
    """
    return Element(name='subcategory', value='MONTHLY_LIMIT_REACHED')


@pytest.fixture()
def error_event(msg_daily_reached):
    """
    Error indicating that too many requests were made
    """
    return Event(type_=Event.RESPONSE,
                 msgs=[
                     msg_daily_reached,
                     ],
                 )


@pytest.fixture()
def non_error_message():
    """
    ???
    """
    return Message(name="validMessage",
                   value='',
                   children={
                       "validMessage": element_daily_reached
                       }
                   )


@pytest.fixture()
def market_data():
    """
    just random data in subscriber
    """
    mk_data = 'INITPAINT'
    bid = 133.75
    ask_size = 1
    ind_bid_flag = False
    return mk_data, bid, ask_size, ind_bid_flag


@pytest.fixture()
def start_subscribe_event():
    """
    SubscriptionStarted = {
        exceptions[] = {
        }
        streamIds[] = {
            "1"
        }
        receivedFrom = {
            address = "localhost:8194"
        }
        reason = "Subscriber made a subscription"
    }
    """
    event_ = Event(type_=Event.SUBSCRIPTION_STATUS,
                   msgs=[Message(value=0, name='SubscriptionStarted'), ]
                   )
    return event_


@pytest.fixture()
def market_data_event(market_data):
    """
    simple example date from subscriber
    """
    mk_data, bid, ask_size, ind_bid_flag = market_data
    msgs = [Message(name="MarketDataEvents",
                    value=[],
                    children={
                        'MarketDataEvents':
                            Element(
                                'MarketDataEvents',
                                value=[],
                                children={
                                    'MKTDATA':      Element('MKTDATA', mk_data),
                                    'BID':          Element("BID", bid),
                                    'ASK_SIZE':     Element('ASK_SIZE',
                                                            ask_size),
                                    'IND_BID_FLAG': Element('IND_BID_FLAG',
                                                            ind_bid_flag)
                                    },
                                )
                        },
                    correlationId=CorrelationId(None),
                    )]

    return Event(Event.SUBSCRIPTION_DATA, msgs)


@pytest.fixture()
def simple_field_data():
    field_name = 'PX_LAST'
    field_value = 10.2
    security_id = 'F Equity'

    return field_name, field_value, security_id


@pytest.fixture()
def simple_field(simple_field_data):
    name, value, _ = simple_field_data

    return Element(name, value)


@pytest.fixture()
def one_value_array_field_data():
    field_name = 'BLOOMBERG_PEERS'
    field_values = ['GM US', 'TSLA US']
    security_id = 'AAPL Equity'

    return field_name, field_values, security_id


@pytest.fixture()
def one_value_array_field(one_value_array_field_data):
    field_name, field_values, _ = one_value_array_field_data

    fields = [Element('Peer ticker', value)
              for value in field_values]

    array_elements = [Element(field_name, None, {field_name: element})
                      for element in fields]

    top_level_element = Element(field_name, None, array_elements)

    return top_level_element


@pytest.fixture()
def multi_value_array_field_data():
    field_name = 'INDX_MWEIGHT'
    field_values = [
        {
            'Member Ticker and Exchange Code': "1332 JT",
            'Percentage Weight ':              0.108078
            },
        {
            'Member Ticker and Exchange Code': "1333 JT",
            'Percentage Weight ':              0.048529
            },
        ]
    security_id = '/bbgid/BBG000HX8KM1'

    return field_name, field_values, security_id


@pytest.fixture()
def multi_value_array_field(multi_value_array_field_data):
    field_name, field_values, _ = multi_value_array_field_data

    dict_elements = [{name: Element(name, value)
                      for name, value in values_dict.items()}
                     for values_dict in field_values]

    array_elements = [Element(field_name, None, dict_element)
                      for dict_element in dict_elements]

    top_level_element = Element(field_name, None, array_elements)

    return top_level_element


@pytest.fixture()
def security_data_array(one_value_array_field,
                        one_value_array_field_data):
    field_name, field_values, security_id = one_value_array_field_data

    field_data = Element(FIELD_DATA, None, [one_value_array_field])
    security = Element(SECURITY, security_id)

    security_data = Element(SECURITY_DATA, None,
                            {
                                SECURITY:   security,
                                FIELD_DATA: field_data,
                                })

    return security_data


@pytest.fixture()
def security_data_simple(simple_field,
                         simple_field_data):
    field_name, field_values, security_id = simple_field_data

    field_data = Element(FIELD_DATA, None, [simple_field])
    security = Element(SECURITY, security_id)

    security_data = Element(SECURITY_DATA, None,
                            {
                                SECURITY:   security,
                                FIELD_DATA: field_data,
                                })

    return security_data


@pytest.fixture()
def security_data_historical(simple_field_data):
    field_name, field_value, security_id = simple_field_data

    value_element = Element(field_name, field_value)
    date_element = Element('date', dt.date(2018, 1, 1))

    field_data = Element(FIELD_DATA, None,
                         {
                             field_name: value_element,
                             'date':     date_element,
                             })

    field_data_sequence = Element(FIELD_DATA, None, [field_data])

    security = Element(SECURITY, security_id)

    security_data = Element(SECURITY_DATA, None,
                            {
                                SECURITY:   security,
                                FIELD_DATA: field_data_sequence,
                                })

    return security_data


@pytest.fixture()
def security_data_with_type(simple_field,
                            simple_field_data):
    field_name, field_values, security_id = simple_field_data

    field_data = Element(FIELD_DATA, None, [simple_field])
    security = Element(SECURITY, '/isin/' + security_id)

    security_data = Element(SECURITY_DATA, None,
                            {
                                SECURITY:   security,
                                FIELD_DATA: field_data,
                                })

    return security_data


@pytest.fixture()
def response_msg_one_security(security_data_array):
    children = Element(SECURITY_DATA, None, [security_data_array])

    return Message('Response', None, {SECURITY_DATA: children})


@pytest.fixture()
def response_event(response_msg_one_security):
    event = Event('RESPONSE', [response_msg_one_security])
    return event


@pytest.fixture()
def response_msg_several_securities(security_data_array,
                                    security_data_simple):
    children = Element(SECURITY_DATA, None, [security_data_array,
                                             security_data_simple])

    return Message('Response', None, {SECURITY_DATA: children})


@pytest.fixture()
def field_exceptions(simple_field_data):
    field_name, _, _ = simple_field_data

    field_id = Element(FIELD_ID, field_name)
    message = Element(MESSAGE, 'Invalid field')
    error_info = Element(ERROR_INFO, None, {MESSAGE: message})

    field_exception = Element(FIELD_EXCEPTIONS, None,
                              {
                                  FIELD_ID:   field_id,
                                  ERROR_INFO: error_info,
                                  })

    field_exceptions = Element(FIELD_EXCEPTIONS, None, [field_exception])
    return field_exceptions


@pytest.fixture()
def security_data_with_field_exception(simple_field_data, field_exceptions):
    field_name, _, security_id = simple_field_data

    security = Element(SECURITY, security_id)

    security_data = Element(SECURITY_DATA, None,
                            {
                                SECURITY:         security,
                                FIELD_EXCEPTIONS: field_exceptions,
                                })

    return security_data


@pytest.fixture()
def security_data_with_security_error(simple_field_data):
    field_name, _, security_id = simple_field_data

    security = Element(SECURITY, security_id)
    security_error = Element(SECURITY_ERROR, 'Invalid security')

    security_data = Element(SECURITY_DATA, None,
                            {
                                SECURITY:       security,
                                SECURITY_ERROR: security_error,
                                })

    return security_data


@pytest.fixture()
def field_search_msg():
    field_info = Element('fieldInfo', None, {
        'description':  Element('description', 'Theta Last Price'),
        'mnemonic':     Element('mnemonic', 'THETA_LAST'),
        'datatype':     Element('datatype', 'Double'),
        'categoryName': Element('categoryName', None, [Element('')]),
        })

    field_data = Element('fieldData', None, {
        'id':        Element('id', 'OP179'),
        'fieldInfo': field_info,
        })

    field_data_array = Element('fieldData', None, [field_data])

    category = Element('category', None, {
        'fieldData': field_data_array,
        })

    category_array = Element('category', None, [category])

    message = Message('categorizedFieldResponse', None,
                      {'category': category_array})

    return message


@pytest.fixture()
def security_lookup_msg():
    security = Element('security', 'F US Equity')
    description = Element('description', 'Ford Motors Co')

    element = Element('element', None, {
        'security':    security,
        'description': description,
        })

    results_array = Element('results', None, [element])

    message = Message('securityLookupResponse', None,
                      {'results': results_array})

    return message


@pytest.fixture()
def curve_lookup_msg():
    description = Element('description', 'GOLD')
    country = Element('country', 'US')
    currency = Element('currency', 'USD')
    curveid = Element('curveid', 'CD1016')
    type_ = Element('type', 'CORP')
    subtype = Element('subtype', 'CDS')
    publisher = Element('publisher', 'Bloomberg')
    bbgid = Element('bbgid', 'YCCD1016')

    element = Element('element', None, {
        'description': description,
        'country':     country,
        'currency':    currency,
        'curveid':     curveid,
        'type':        type_,
        'subtype':     subtype,
        'publisher':   publisher,
        'bbgid':       bbgid,
        })

    results_array = Element('results', None, [element])

    message = Message('curveLookupResponse', None,
                      {'results': results_array})

    return message


@pytest.fixture()
def government_lookup_msg():
    parseky = Element('parseky', 'T')
    name = Element('name', 'Treasuries')
    ticker = Element('ticker', 'T')

    element = Element('element', None, {
        'parseky': parseky,
        'name':    name,
        'ticker':  ticker,
        })

    results_array = Element('results', None, [element])

    message = Message('govtLookupResponse', None,
                      {'results': results_array})

    return message
