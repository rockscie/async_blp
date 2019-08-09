import pandas as pd
import pytest

from async_blp.env_test import Element
from async_blp.requests import FIELD_DATA
from async_blp.requests import ReferenceDataRequest
from async_blp.requests import SECURITY
from async_blp.requests import SECURITY_DATA


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
    security_id = 'F Equity'

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
def security_data(one_value_array_field,
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
                                   security_data,
                                   one_value_array_field_data,
                                   ):
        field_name, field_values, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        required_df = pd.DataFrame([[field_values]],
                                   index=[security_id],
                                   columns=[field_name],
                                   )

        actual_df = request._parse_security_data(security_data)

        pd.testing.assert_frame_equal(actual_df, required_df)
