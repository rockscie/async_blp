"""
String optimizaton for blpapi
"""

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

REFERENCE_DATA_RESPONSE = blpapi.Name("ReferenceDataResponse")
HISTORICAL_DATA_RESPONSE = blpapi.Name("HistoricalDataResponse")
MARKET_DATA_EVENTS = blpapi.Name("MarketDataEvents")
RESPONSE_ERROR = blpapi.Name("responseError")
SECURITY_DATA = blpapi.Name("securityData")
SECURITY_ERROR = blpapi.Name("securityError")
ID = blpapi.Name("id")
ERROR_INFO = blpapi.Name("errorInfo")
MESSAGE = blpapi.Name("message")
FIELD_ID = blpapi.Name('fieldId')
FIELD_MNEMONIC = blpapi.Name("mnemonic")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_INFO = blpapi.Name('fieldInfo')
FIELD_DATA_TYPE = blpapi.Name('datatype')
FIELD_DESC = blpapi.Name("description")
FIELD_DOC = blpapi.Name("documentation")
SECURITY = blpapi.Name("security")
CATEGORY = blpapi.Name("category")
CATEGORY_NAME = blpapi.Name("categoryName")
CATEGORY_ID = blpapi.Name("categoryId")
EXCLUDE = blpapi.Name("exclude")
FIELD_EXCEPTIONS = blpapi.Name('fieldExceptions')
