import asyncio
import datetime as dt

import pandas as pd
import pytest

from async_blp.enums import ErrorBehaviour
from async_blp.enums import SecurityIdType
from async_blp.errors import BloombergErrors
from async_blp.errors import ErrorType
from async_blp.requests import HistoricalDataRequest
from async_blp.requests import ReferenceDataRequest
from async_blp.utils.blp_name import ERROR_INFO
from async_blp.utils.blp_name import FIELD_DATA
from async_blp.utils.blp_name import FIELD_EXCEPTIONS
from async_blp.utils.blp_name import FIELD_ID
from async_blp.utils.blp_name import MESSAGE
from async_blp.utils.blp_name import SECURITY
from async_blp.utils.blp_name import SECURITY_DATA
from async_blp.utils.blp_name import SECURITY_ERROR
from async_blp.utils.env_test import Element
from async_blp.utils.env_test import Message
from async_blp.utils.exc import BloombergException


# pylint does not like pytest.fixture but we do
# pylint: disable=redefined-outer-name

# we need protected access in tests
# pylint: disable=protected-access


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


class TestReferenceDataRequest:

    def test___parse_field_data__simple_field(self,
                                              simple_field,
                                              simple_field_data):
        name, value, security = simple_field_data

        request = ReferenceDataRequest([security], [name])

        parsed_name, parsed_value = request._parse_field_data(simple_field)
        assert parsed_name == name
        assert parsed_value == value

    def test___parse_field_data__array_field(self,
                                             one_value_array_field_data,
                                             one_value_array_field):
        name, value, security = one_value_array_field_data

        request = ReferenceDataRequest([security], [name])

        parsed_name, parsed_value = request._parse_field_data(
            one_value_array_field)
        assert parsed_name == name
        assert parsed_value == value

    def test___parse_array_field__one_value(self,
                                            one_value_array_field_data,
                                            one_value_array_field):
        field_name, field_values, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        parsed_name, parsed_values = request._parse_array_field(
            one_value_array_field)

        assert parsed_name == field_name
        assert parsed_values == field_values

    def test___parse_array_field__multi_value(self,
                                              multi_value_array_field_data,
                                              multi_value_array_field):
        field_name, field_values, security_id = multi_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        parsed_name, parsed_values = request._parse_array_field(
            multi_value_array_field)

        assert parsed_name == field_name
        assert parsed_values == field_values

    def test___parse_security_data(self,
                                   security_data_array,
                                   one_value_array_field_data,
                                   ):
        field_name, field_values, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        required_df = pd.DataFrame([[field_values]],
                                   index=[security_id],
                                   columns=[field_name],
                                   )

        actual_df = request._parse_security_data(security_data_array)

        pd.testing.assert_frame_equal(actual_df, required_df)

    def test__init__not_inside_loop(self,
                                    simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        assert request._loop is None
        assert request._msg_queue is None

    @pytest.mark.asyncio
    async def test__set_running_loop_as_default__queue_is_empty(
            self,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        request.set_running_loop_as_default()

        assert request._loop == asyncio.get_running_loop()

    @pytest.mark.asyncio
    async def test__set_running_loop_as_default__queue_is_not_empty(
            self,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])
        request._msg_queue.put_nowait(1)

        with pytest.raises(RuntimeError):
            request.set_running_loop_as_default()

    def test__set_running_loop_as_default__not_inside_loop(
            self,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        with pytest.raises(RuntimeError):
            request.set_running_loop_as_default()

    @pytest.mark.asyncio
    async def test__send_queue_message__inside_loop(self, simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        request.send_queue_message(1)
        await asyncio.sleep(0.001)

        assert request._msg_queue.get_nowait() == 1

    def test__send_queue_message__not_inside_loop(self, simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        with pytest.raises(RuntimeError):
            request.send_queue_message(1)

    def test___parse_field_exceptions(self,
                                      field_exceptions,
                                      simple_field_data):
        field_name, _, security_id = simple_field_data

        errors = ReferenceDataRequest._parse_field_exceptions(security_id,
                                                              field_exceptions,
                                                              )
        expected_errors = {
            (security_id, field_name):
                ErrorType.INVALID_FIELD_HISTORICAL
            }

        assert errors == expected_errors

    def test___parse_errors__ignore_errors(self,
                                           simple_field_data,
                                           security_data_with_field_exception):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name],
                                       error_behavior=ErrorBehaviour.IGNORE)

        errors = request._parse_errors(security_data_with_field_exception)

        assert errors is None

    def test___parse_errors__raise_errors(self,
                                          simple_field_data,
                                          security_data_with_field_exception):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name],
                                       error_behavior=ErrorBehaviour.RAISE)

        with pytest.raises(BloombergException):
            request._parse_errors(security_data_with_field_exception)

    def test___parse_errors__return_errors__field_exception(
            self,
            simple_field_data,
            security_data_with_field_exception):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name],
                                       error_behavior=ErrorBehaviour.RETURN)

        errors = request._parse_errors(security_data_with_field_exception)

        expected_errors = BloombergErrors([], {
            (security_id, field_name): ErrorType.INVALID_FIELD_HISTORICAL,
            })

        assert errors == expected_errors

    def test___parse_errors__return_errors__security_error(
            self,
            simple_field_data,
            security_data_with_security_error):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name],
                                       error_behavior=ErrorBehaviour.RETURN)

        errors = request._parse_errors(security_data_with_security_error)
        expected_errors = BloombergErrors([security_id], {})

        assert errors == expected_errors

    @pytest.mark.asyncio
    async def test__process__one_security(self,
                                          response_msg_one_security,
                                          one_value_array_field_data):
        field_name, field_value, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        request.send_queue_message(response_msg_one_security)
        request.send_queue_message(None)

        expected_df = pd.DataFrame(columns=[field_name], index=[security_id])
        expected_df.at[security_id, field_name] = field_value

        actual_df, _ = await request.process()

        pd.testing.assert_frame_equal(actual_df, expected_df)

    @pytest.mark.asyncio
    async def test__process__several_securities(self,
                                                response_msg_several_securities,
                                                one_value_array_field_data,
                                                simple_field_data):
        field_name_1, field_value_1, security_id_1 = one_value_array_field_data
        field_name_2, field_value_2, security_id_2 = simple_field_data

        request = ReferenceDataRequest([security_id_1, security_id_2],
                                       [field_name_1, field_name_2])

        request.send_queue_message(response_msg_several_securities)
        request.send_queue_message(None)

        expected_df = pd.DataFrame(columns=[field_name_1, field_name_2],
                                   index=[security_id_1, security_id_2])
        expected_df.at[security_id_1, field_name_1] = field_value_1
        expected_df.at[security_id_2, field_name_2] = field_value_2

        actual_df, _ = await request.process()

        pd.testing.assert_frame_equal(actual_df, expected_df)

    @pytest.mark.asyncio
    async def test__process__empty(self, one_value_array_field_data):
        field_name, _, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])
        request.send_queue_message(None)

        expected_df = pd.DataFrame(columns=[field_name], index=[security_id])

        actual_df, _ = await request.process()
        pd.testing.assert_frame_equal(actual_df, expected_df)

    def test___get_security_id_from_security_data__type_is_none(
            self,
            security_data_simple,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        parsed_security_id = request._get_security_id_from_security_data(
            security_data_simple)

        assert security_id == parsed_security_id

    def test___get_security_id_from_security_data__type_is_not_none(
            self,
            security_data_with_type,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name],
                                       SecurityIdType.ISIN)

        parsed_security_id = request._get_security_id_from_security_data(
            security_data_with_type)

        assert security_id == parsed_security_id


class TestHistoricalDataRequest:

    def test__weight(self):
        securities = ['security_1', 'security_2', 'security_3']
        fields = ['field_1', 'field_2', 'field_3']
        start_date = dt.date(2018, 1, 1)
        end_date = dt.date(2018, 1, 10)

        request = HistoricalDataRequest(securities, fields,
                                        start_date, end_date)

        assert request.weight == 3 * 3 * 9

    def test___parse_security_data(self,
                                   simple_field_data,
                                   security_data_historical):
        field_name, field_value, security_id = simple_field_data
        date = dt.date(2018, 1, 1)

        request = HistoricalDataRequest([security_id], [field_name],
                                        date, dt.date(2018, 1, 4))

        parsed_df = request._parse_security_data(security_data_historical)

        index = pd.MultiIndex.from_tuples([(pd.Timestamp(date), security_id)],
                                          names=['date', 'security'])
        expected_df = pd.DataFrame([field_value],
                                   index=index,
                                   columns=[field_name])

        pd.testing.assert_frame_equal(parsed_df, expected_df)
