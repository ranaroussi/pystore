# PyStore - Flat-file datastore for timeseries data

[![Python version](https://img.shields.io/pypi/pyversions/pystore.svg?maxAge=60)](https://pypi.python.org/pypi/pystore)
[![Travis-CI build status](https://img.shields.io/travis/ranaroussi/pystore/master.svg?)](https://travis-ci.org/ranaroussi/pystore)
[![PyPi version](https://img.shields.io/pypi/v/pystore.svg?maxAge=60)](https://pypi.python.org/pypi/pystore)
[![PyPi status](https://img.shields.io/pypi/status/pystore.svg?maxAge=60)](https://pypi.python.org/pypi/pystore)
[![Star this repo](https://img.shields.io/github/stars/ranaroussi/pystore.svg?style=social&label=Star&maxAge=60)](https://github.com/ranaroussi/pystore)
[![Follow me on twitter](https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow%20Me&maxAge=60)](https://twitter.com/aroussi)


[PyStore](https://github.com/ranaroussi/pystore) is a Pythonic, flat-file
datastore for timeseries data. It's built on top of great libraries,
including [Pandas](http://pandas.pydata.org/), [Numpy](http://numpy.pydata.org/),
[Dask](http://dask.pydata.org/), and [Parquet](http://parquet.apache.org)
(via [Fastparquet](https://github.com/dask/fastparquet)),
to provide an easy to use datastore for Python developers that can easily
query millions of rows per second per client.

PyStore is hugely inspired by [Man AHL](http://www.ahl.com/)'s
[Arctic](https://github.com/manahl/arctic). We highly reommend you check it out.


## Quickstart

### Install PyStore

``` bash
$ pip install PyStore
```

### Using PyStore

``` python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pystore
import quandl

# Connect to local datastore
store = pystore.Store('mydatastore')
# default path is `~/.pystore`, otherwise:
# store = pystore.Store('mydatastore', path='/usr/share/pystore')

# List existing collections
store.list_collections()

# Create a collection
store.create_collection('NASDAQ')

# Access the collection
collection = store.collection('NASDAQ')

# List items in collection
collection.list_items()

# Load some data from Quandl
aapl = quandl.get("WIKI/AAPL", authtoken="your token here")

# Store the first 100 rows of the data in the collection under "AAPL"
collection.write('AAPL', aapl[:100], metadata={'source': 'Quandl'})

# Reading the item's data
item = collection.item('AAPL')
data = item.data  # <-- Dask dataframe (see dask.pydata.org)
metadata = item.metadata
df = item.to_pandas()

# Append the rest of the rows to the "AAPL" item
collection.append('AAPL', aapl[100:])

# Reading the item's data
item = collection.item('AAPL')
data = item.data
metadata = item.metadata
df = item.to_pandas()

```


## Concepts

PyStore provides namespaced *collections* of data.
These collections allow bucketing data by *source*, *user* or some other metric
(for example frequency: End-Of-Day; Minute Bars; etc.). Each collection (or namespace)
maps to a directory containing partitioned **parquet files** for each item (e.g. symbol).

A good practice it to create collections that may look something like this:

* collection.EOD
* collection.ONEMINUTE


## Requirements

PyStore currently works with:

* Python 3.5 or higher
* Pandas
* Numpy
* Dask
* Fastparquet

Tested to work on:

* Linux
* Unix
* macOS



## Acknowledgements

PyStore is hugely inspired by [Man AHL](http://www.ahl.com/)'s [Arctic](https://github.com/manahl/arctic). We highly reommend you check it out.

Contributions welcome!


## License

PyStore is licensed under the **GNU Lesser General Public License v2.1**. A copy of which is included in [LICENSE](LICENSE).

