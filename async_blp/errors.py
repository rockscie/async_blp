import enum
from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union


class ErrorType(str, enum.Enum):
    # both reference and historical requests
    # security id is incorrect
    INVALID_SECURITY = 'INVALID_SECURITY'

    # reference data request
    # field is not correct
    INVALID_FIELD = 'Field not valid'

    # reference data request
    # field is correct but not applicable to the given security
    FIELD_NOT_APPLICABLE = 'Field not applicable to security'

    # historical data request
    # field is incorrect
    INVALID_FIELD_HISTORICAL = 'Invalid field'

    # historical data request
    # field is either not applicable to security or is does not have
    # historical data
    INVALID_HISTORICAL_FIELD = 'Not valid historical field'


@dataclass
class BloombergErrors:
    invalid_securities: List[str] = field(default_factory=list)

    # { (security, field) : error }
    invalid_fields: Dict[Tuple[str, str], str] = field(default_factory=dict)

    def get_errors_by_security(self,
                               security_id: str,
                               ) -> Union[ErrorType, Dict[str, ErrorType]]:

        if security_id in self.invalid_securities:
            return ErrorType.INVALID_SECURITY

        field_errors = {field: error
                        for (security, field), error
                        in self.invalid_fields.items()
                        if security == security_id}

        return field_errors

    def get_errors_by_field(self, field_name: str) -> Dict[str, ErrorType]:
        field_errors = {security: error
                        for (security, field), error
                        in self.invalid_fields.items()
                        if field == field_name}

        return field_errors

    def __add__(self, other: 'BloombergErrors'):
        invalid_securities = list(set(self.invalid_securities
                                      + other.invalid_securities))

        invalid_fields = self.invalid_fields.copy()
        invalid_fields.update(other.invalid_fields)

        new_errors = BloombergErrors(invalid_securities, invalid_fields)

        return new_errors
