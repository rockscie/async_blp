# Welcome to async_blp

Our goal is to create simple and fast Bloomberg Open API wrapper that can be used in highload environments. 
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

- Bloomberg API for Python (more info [here](https://www.bloomberg.com/professional/support/api-library/))
```cmd
python -m pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
```

