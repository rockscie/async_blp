import enum


class SecurityIdType(enum.Enum):
    """
    Some of the possible security identifier types. For more information see
    https://www.bloomberg.com/professional/support/api-library/
    """
    TICKER = '/ticker/'
    ISIN = '/isin/'
    CUSIP = '/cusip/'
    SEDOL = '/sedol/'

    BL_SECURITY_IDENTIFIER = '/bsid/'
    BL_SECURITY_SYMBOL = '/bsym/'
    BL_UNIQUE_IDENTIFIER = '/buid/'
    BL_GLOBAL_IDENTIFIER = '/bbgid'

    def __str__(self):
        return self.value


class ErrorBehaviour(enum.Enum):
    """
    Enum of supported error behaviours.

    RAISE - raise exception when Bloomberg reports an error
    RETURN - return all errors in a separate dict
    IGNORE - ignore all errors
    WARN - print warnings
    """
    RAISE = 'raise'
    RETURN = 'return'
    IGNORE = 'ignore'
    WARN = 'warn'