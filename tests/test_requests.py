import async_blp.env_test as blpapi
from async_blp.requests import ReferenceDataRequest


def simple_field(field_name, field_value):
    lowest_element = blpapi.Element(field_name, field_value)

    field_data = blpapi.Element('fieldData', None, {'': lowest_element})

    return field_data


def array_field_one_value(field_name, values):
    fields = {num: blpapi.Element(field_name, value)
              for num, value in enumerate(values)}

    array_field = blpapi.Element(field_name, None, fields)


class TestReferenceDataRequest:

    def test___parse_field_data__simple_field(self):
        field_name = 'PX_LAST'
        field_value = 10.2
        security_id = 'F Equity'

        field = blpapi.Element(field_name, field_value)

        request = ReferenceDataRequest([security_id], [field_name])

        parsed_name, parsed_value = request._parse_field_data(field)
        assert parsed_name == field_name
        assert parsed_value == field_value

    def test___parse_array_field(self):
        field_name = 'PX_LAST'
        field_value = 10.2
        security_id = 'F Equity'
