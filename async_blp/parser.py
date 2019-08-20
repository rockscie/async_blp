import datetime as dt
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pandas as pd

from .enums import ErrorBehaviour
from .enums import SecurityIdType
from .errors import BloombergErrors
from .errors import ErrorType
from .utils.blp_name import ERROR_INFO
from .utils.blp_name import FIELD_DATA
from .utils.blp_name import FIELD_EXCEPTIONS
from .utils.blp_name import FIELD_ID
from .utils.blp_name import MESSAGE
from .utils.blp_name import SECURITY
from .utils.blp_name import SECURITY_ERROR
from .utils.exc import BloombergException

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

BloombergValue = Union[str, int, float, dt.date, dt.datetime,
                       Dict[str, Union[str, int, float, dt.date, dt.datetime]]]


def get_security_id_from_security_data(
        security_data: blpapi.Element,
        security_id_type: Optional[SecurityIdType] = None,
        ):
    """
    Retrieve security id from security data and remove type prefix if needed
    """
    security_id = security_data.getElementAsString(SECURITY)

    if security_id_type is not None:
        security_id = security_id_type.remove_type(security_id)

    return security_id


def parse_reference_security_data(security_data) -> pd.DataFrame:
    """
    Parse single security data element.

    Return pd.DataFrame with one row and multiple columns corresponding
    to the received fields.
    """
    security_id = get_security_id_from_security_data(security_data)

    field_data: blpapi.Element = security_data.getElement(FIELD_DATA)

    security_df = pd.DataFrame()

    for field in field_data.elements():
        field_name, field_value = parse_field_data(field)

        if field_name not in security_df and isinstance(field_value, list):
            security_df[field_name] = pd.Series().astype(object)

        security_df.at[security_id, field_name] = field_value

    return security_df


def parse_errors(security_data: blpapi.Element,
                 error_behaviour: ErrorBehaviour) -> Optional[BloombergErrors]:
    """
    Check if the given security data has any errors and process them
    according to `self._error_behaviour`

    Return None if exceptions are ignored, otherwise return
    BloombergErrors instance
    """
    if error_behaviour == ErrorBehaviour.IGNORE:
        return None

    security_id = get_security_id_from_security_data(security_data)
    security_errors = BloombergErrors()

    if security_data.hasElement(SECURITY_ERROR):
        security_errors.invalid_securities.append(security_id)

    if security_data.hasElement(FIELD_EXCEPTIONS):
        field_exceptions = security_data.getElement(FIELD_EXCEPTIONS)
        field_errors = parse_field_exceptions(security_id,
                                              field_exceptions)

        if field_errors:
            security_errors.invalid_fields.update(field_errors)

    if error_behaviour == ErrorBehaviour.RAISE and security_errors:
        raise BloombergException(security_errors)

    return security_errors


def parse_field_exceptions(security_id: str,
                           field_exceptions: blpapi.Element,
                           ) -> Dict[Tuple[str, str], str]:
    """
    Parse field exceptions for one security.

    Return dict {(security_id, field name) : error message}
    """
    errors = {}

    for error in field_exceptions.values():
        field = error.getElementAsString(FIELD_ID)
        error_info = error.getElement(ERROR_INFO)
        message = error_info.getElementAsString(MESSAGE)

        try:
            message = ErrorType(message)
        except ValueError:
            pass

        errors[(security_id, field)] = message

    return errors


def parse_field_data(field: blpapi.Element,
                     ) -> Tuple[str,
                                Union[BloombergValue,
                                      List[BloombergValue],
                                      List[Dict[str, BloombergValue]]]]:
    """
    Parse single field data element.

    If field data contains bulk data, return list of dicts or list of
    values. Otherwise, return single value
    """

    if field.isArray():
        return parse_array_field(field)

    field_name = str(field.name())
    field_value = field.getValue()
    return field_name, field_value


def parse_array_field(field: blpapi.Element,
                      ) -> Tuple[str,
                                 Union[List[BloombergValue],
                                       List[Dict[str, BloombergValue]]]]:
    """
    Parse single field that contains bulk data.

    Return field name and field values, either as list of dicts or
    list of values.
    """
    field_name = str(field.name())

    values = [
        {
            str(e1.name()): e1.getValue()
            for e1 in elem.elements()
            }
        for elem in field.values()
        ]

    if values and len(values[0]) == 1:
        values = [list(value.values())[0]
                  for value in values]

    return field_name, values


def parse_historical_security_data(security_data) -> pd.DataFrame:
    security_id = get_security_id_from_security_data(security_data)

    field_data: blpapi.Element = security_data.getElement(FIELD_DATA)

    empty_index = pd.MultiIndex.from_tuples([], names=['date', 'security'])
    security_df = pd.DataFrame(index=empty_index)

    for fields_sequence in field_data.values():
        fields_dict = {}

        for field in fields_sequence.elements():
            field_name, field_value = parse_field_data(field)
            fields_dict[field_name] = field_value

        date = pd.Timestamp(fields_dict['date'])
        for name, value in fields_dict.items():
            if name == 'date':
                continue

            security_df.at[(date, security_id), name] = value

    return security_df
