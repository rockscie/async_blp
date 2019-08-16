from async_blp.errors import BloombergErrors
from async_blp.errors import ErrorType


class TestBloombergErrors:

    def test__get_errors_by_security__invalid_security(self):
        errors = BloombergErrors(['invalid security'])

        actual_errors_by_security = errors.get_errors_by_security(
            'invalid security')

        assert actual_errors_by_security == ErrorType.INVALID_SECURITY

    def test__get_errors_by_security__invalid_fields(self):
        errors = BloombergErrors(invalid_fields={
            ('security', 'field'): 'Field not valid',
            })

        actual_errors_by_security = errors.get_errors_by_security('security')

        assert actual_errors_by_security == {'field': 'Field not valid'}

    def test__get_errors_by_field(self):
        errors = BloombergErrors(invalid_fields={
            ('security', 'field'): 'Field not valid',
            })

        actual_errors_by_security = errors.get_errors_by_field('field')

        assert actual_errors_by_security == {'security': 'Field not valid'}

    def test__add(self):
        errors_1 = BloombergErrors(['security_1'], {
            ('security_1', 'field_1'): 'Field not valid',
            })

        errors_2 = BloombergErrors(invalid_fields={
            ('security_1', 'field_2'): 'Field not valid',
            })

        errors_sum = errors_1 + errors_2

        assert errors_sum.invalid_securities == ['security_1']
        assert errors_sum.invalid_fields == {
            ('security_1', 'field_1'): 'Field not valid',
            ('security_1', 'field_2'): 'Field not valid',
            }
