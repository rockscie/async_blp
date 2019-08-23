# async_blp

[![Build Status](https://travis-ci.com/rockscie/async_blp.svg?branch=master)](https://travis-ci.com/rockscie/async_blp)

## Overview

The goal of `async_blp` is to create simple and fast Bloomberg Open API wrapper that can be used in highload environments. 
It allows asynchronous processing of hundreds of 
Bloomberg request simultaneously. Currently `async_blp` provides support for reference and historical data, 
instruments lookup and field search, as well as subscriptions.
More request types will be added in the future.

## Features

- *Fast*. Using `asyncio` allows to process Bloomberg requests simultaneously while creating little overhead
- *Simple*. `async_blp` takes care of creating and managing Bloomberg sessions as well as of parsing responses
- *User-friendly output*. Data is returned as a `pandas.DataFrame` object
- *Error handling*. Security and field errors are returned in a separate object that can be easily inspected

## Installation

You can install async_blp from PyPI using

```cmd
pip install async_blp
```

## Requirements

- [Python 3.7+](https://www.python.org)

- [Pandas](https://pandas.pydata.org)

- [Bloomberg C++ SDK version 3.12.1+](https://www.bloomberg.com/professional/support/api-library/)

- Bloomberg API for Python (more info here: https://www.bloomberg.com/professional/support/api-library/)
  ```
  python -m pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
  ```

## Documentation

https://async-blp.readthedocs.io/en/latest/

## Examples

Before using **async_blp**, install **blpapi** from the link above and login in the Bloomberg Terminal.

```python
import async_blp

async def blp_example(loop):
    bloomberg = async_blp.AsyncBloomberg(loop=loop)
  
    data, _ = await bloomberg.get_reference_data(['F US Equity'], ['LAST_PRICE'])
  
    # it is important to wait until Bloomberg successfully closes all the sessions
    await bloomberg.stop()
  
    return data
```

More examples can be found here: https://github.com/rockscie/async_blp/tree/master/examples