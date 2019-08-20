import datetime as dt

import pandas as pd
import pytest

from async_blp.enums import ErrorBehaviour
from async_blp.enums import SecurityIdType
from async_blp.errors import BloombergErrors
from async_blp.errors import ErrorType
from async_blp.parser import get_security_id_from_security_data
from async_blp.parser import parse_array_field
from async_blp.parser import parse_errors
from async_blp.parser import parse_field_data
from async_blp.parser import parse_field_exceptions
from async_blp.parser import parse_historical_security_data
from async_blp.parser import parse_reference_security_data
from async_blp.utils.exc import BloombergException


def test___parse_field_data__simple_field(simple_field,
                                          simple_field_data):
    name, value, _ = simple_field_data

    parsed_name, parsed_value = parse_field_data(simple_field)
    assert parsed_name == name
    assert parsed_value == value


def test___parse_field_data__array_field(one_value_array_field_data,
                                         one_value_array_field):
    name, value, _ = one_value_array_field_data

    parsed_name, parsed_value = parse_field_data(
        one_value_array_field)
    assert parsed_name == name
    assert parsed_value == value


def test___parse_array_field__one_value(one_value_array_field_data,
                                        one_value_array_field):
    field_name, field_values, _ = one_value_array_field_data

    parsed_name, parsed_values = parse_array_field(
        one_value_array_field)

    assert parsed_name == field_name
    assert parsed_values == field_values


def test___parse_array_field__multi_value(multi_value_array_field_data,
                                          multi_value_array_field):
    field_name, field_values, _ = multi_value_array_field_data

    parsed_name, parsed_values = parse_array_field(
        multi_value_array_field)

    assert parsed_name == field_name
    assert parsed_values == field_values


def test___parse_security_data(security_data_array,
                               one_value_array_field_data,
                               ):
    field_name, field_values, security_id = one_value_array_field_data

    required_df = pd.DataFrame([[field_values]],
                               index=[security_id],
                               columns=[field_name],
                               )

    actual_df = parse_reference_security_data(security_data_array)

    pd.testing.assert_frame_equal(actual_df, required_df)


def test___parse_field_exceptions(field_exceptions,
                                  simple_field_data):
    field_name, _, security_id = simple_field_data

    errors = parse_field_exceptions(security_id, field_exceptions)

    expected_errors = {
        (security_id, field_name):
            ErrorType.INVALID_FIELD_HISTORICAL
        }

    assert errors == expected_errors


def test___parse_errors__ignore_errors(simple_field_data,
                                       security_data_with_field_exception):
    field_name, _, security_id = simple_field_data

    errors = parse_errors(security_data_with_field_exception,
                          ErrorBehaviour.IGNORE)

    assert errors is None


def test___parse_errors__raise_errors(simple_field_data,
                                      security_data_with_field_exception):
    field_name, _, _ = simple_field_data

    with pytest.raises(BloombergException):
        parse_errors(security_data_with_field_exception, ErrorBehaviour.RAISE)


def test___parse_errors__return_errors__field_exception(
        simple_field_data,
        security_data_with_field_exception):
    field_name, _, security_id = simple_field_data

    errors = parse_errors(security_data_with_field_exception,
                          ErrorBehaviour.RETURN)

    expected_errors = BloombergErrors([], {
        (security_id, field_name): ErrorType.INVALID_FIELD_HISTORICAL,
        })

    assert errors == expected_errors


def test___parse_errors__return_errors__security_error(
        simple_field_data,
        security_data_with_security_error):
    field_name, _, security_id = simple_field_data

    errors = parse_errors(security_data_with_security_error,
                          ErrorBehaviour.RETURN)
    expected_errors = BloombergErrors([security_id], {})

    assert errors == expected_errors


def test___get_security_id_from_security_data__type_is_none(
        security_data_simple,
        simple_field_data):
    field_name, _, security_id = simple_field_data

    parsed_security_id = get_security_id_from_security_data(
        security_data_simple)

    assert security_id == parsed_security_id


def test__get_security_id_from_security_data__type_is_not_none(
        security_data_with_type,
        simple_field_data):
    field_name, _, security_id = simple_field_data

    parsed_security_id = get_security_id_from_security_data(
        security_data_with_type, SecurityIdType.ISIN)

    assert security_id == parsed_security_id


def test__parse_historical_security_data(simple_field_data,
                                         security_data_historical):
    field_name, field_value, security_id = simple_field_data
    date = dt.date(2018, 1, 1)

    parsed_df = parse_historical_security_data(security_data_historical)

    index = pd.MultiIndex.from_tuples([(pd.Timestamp(date), security_id)],
                                      names=['date', 'security'])
    expected_df = pd.DataFrame([field_value],
                               index=index,
                               columns=[field_name])

    pd.testing.assert_frame_equal(parsed_df, expected_df)
