## Prerequisites

- Make sure that `blpapi` is installed correctly. It should be imported without any errors.
```python
import blpapi
```


- `async_blp` uses python async framework. If you're unfamiliar with it, 
have a look at async tutorial [here](https://realpython.com/async-io-python/)

## General principles

- `async_blp`, following Bloomberg API, supports two paradigms: request-response and subscription. 
You can read more about their differences and use cases in Bloomberg API Core developer 
guide [here](https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf)

- For performance reasons, `async_blp` does not close sessions automatically when it is done 
processing requests. It allows us to reuse sessions for subsequent requests and reduce overhead. 
However, as each session is run in its own thread, it means that the whole 
application will not finish running until there is at least one opened session left. 
To stop the sessions and to allow your application to finish, use `await bloomberg.stop` (see examples)


## Reference data request
Provides the current value of a security/field pair.

*Input*: list of securities, list of fields

*Output*: tuple consisting of two objects:
 
- `pd.DataFrame` object, where each row is a single security and each column is a field
- `BloombergError` object containing security and field errors, if any has occurred during request

```python
import asyncio
import async_blp

async def blp_example():
    bloomberg = async_blp.AsyncBloomberg()
    
    data, _ = await bloomberg.get_reference_data(['F US Equity'], ['PX_LAST'])
    
    # it is important to wait until Bloomberg successfully closes all the sessions
    await bloomberg.stop()
    
    return data
  
if __name__ == '__main__':
    data = asyncio.run(blp_example())
    print(data)
```
```python
Output[]:
              PX_LAST
F US Equity    8.806
```
## Historical data request
Provides end-of-day data over a defined period of time for a security/field pair.

*Input*: list of securities, list of fields, period start date, period end date

*Output*: tuple consisting of two objects:
 
- `pd.DataFrame` object with Multindex consisting of date and security and columns as fields
- `BloombergError` object containing security and field errors, if any has occurred during request

```python
import asyncio
import datetime as dt
import async_blp


async def blp_historical_example():
    security_id = 'F US Equity'
    field = 'PX_LAST'
    start_date = dt.date(2019, 1, 1)
    end_date = dt.date(2019, 3, 1)

    bloomberg = async_blp.AsyncBloomberg()

    data, errors = await bloomberg.get_historical_data([security_id],
                                                       [field],
                                                       start_date,
                                                       end_date)

    await bloomberg.stop()

    return data, errors
  
if __name__ == '__main__':
    data = asyncio.run(blp_historical_example())
    print(data)
```
```python
Output[]:
                           PX_LAST
date       security           
2019-01-05 F US Equity     NaN
2019-01-06 F US Equity     NaN
2019-01-07 F US Equity    8.29
2019-01-08 F US Equity    8.37
2019-01-09 F US Equity    8.72
2019-01-10 F US Equity    8.67
2019-01-11 F US Equity    8.82
2019-01-12 F US Equity     NaN
```

## Security Lookup Request
The Security Lookup (a.k.a. Instrument Lookup) request constructs a search based upon the 
"query" element's string value, as well as the additional filters that you set, 
such as the yellow key and language override elements.

*Input*: query, max desirable number of results, additional options in a dict

*Output*: tuple consisting of two objects:
 
- `pd.DataFrame` object where each row is one security with `security` and `description` columns
- empty `BloombergError` object

```python
import asyncio
import async_blp


async def blp_security_lookup_example():
    bloomberg = async_blp.AsyncBloomberg()

    data, _ = await bloomberg.security_lookup('Ford', max_results=10)

    await bloomberg.stop()

    return data
  
if __name__ == '__main__':
    data = asyncio.run(blp_security_lookup_example())
    print(data)
```
```python
Output[]:
                security                                        description
0        FORD US<equity>                      Forward Industries Inc (U.S.)
1                F<corp>        Ford Motor Credit Co LLC (Multiple Matches)
2           F US<equity>                 Ford Motor Co Common Shares (U.S.)
3  F CB USD SR 30Y<corp>      Ford Motor Co Generic Benchmark 30Y Corporate
4  F GB USD SR 30Y<corp>      Ford Motor Co Generic Benchmark 30Y Corporate
5  F CB USD SR 10Y<corp>  Ford Motor Credit Co LLC Generic Benchmark 10Y...
6  F GB USD SR 10Y<corp>  Ford Motor Credit Co LLC Generic Benchmark 10Y...
7   F CB USD SR 3Y<corp>  Ford Motor Credit Co LLC Generic Benchmark 3Y ...
8   F GB USD SR 3Y<corp>  Ford Motor Credit Co LLC Generic Benchmark 3Y ...
9   F CB USD SR 5Y<corp>  Ford Motor Credit Co LLC Generic Benchmark 5Y ...
```


## Subscription

WIP

## Specifying security id type

WIP

## Error handling

WIP

## Performance optimization

WIP

