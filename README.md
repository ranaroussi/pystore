# PyStore - Fast data store for Pandas timeseries data

[![Python version](https://img.shields.io/badge/python-2.7,%203.5+-blue.svg?style=flat)](https://pypi.python.org/pypi/pystore)
[![PyPi version](https://img.shields.io/pypi/v/pystore.svg?maxAge=60)](https://pypi.python.org/pypi/pystore)
[![PyPi status](https://img.shields.io/pypi/status/pystore.svg?maxAge=60)](https://pypi.python.org/pypi/pystore)
[![Travis-CI build status](https://img.shields.io/travis/ranaroussi/pystore/master.svg?maxAge=1)](https://travis-ci.com/ranaroussi/pystore)
[![CodeFactor](https://www.codefactor.io/repository/github/ranaroussi/pystore/badge)](https://www.codefactor.io/repository/github/ranaroussi/pystore)
[![Star this repo](https://img.shields.io/github/stars/ranaroussi/pystore.svg?style=social&label=Star&maxAge=60)](https://github.com/ranaroussi/pystore)
[![Follow me on twitter](https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60)](https://twitter.com/aroussi)

<br>

[PyStore](https://github.com/ranaroussi/pystore) is a simple (yet powerful)
datastore for Pandas dataframes, and while it can store any Pandas object,
**it was designed with storing timeseries data in mind**.

It's built on top of [Pandas](http://pandas.pydata.org), [Numpy](http://numpy.pydata.org),
[Dask](http://dask.pydata.org), and [Parquet](http://parquet.apache.org)
(via [Fastparquet](https://github.com/dask/fastparquet)),
to provide an easy to use datastore for Python developers that can easily
query millions of rows per second per client.

==> Check out [this Blog post](https://medium.com/@aroussi/fast-data-store-for-pandas-time-series-data-using-pystore-89d9caeef4e2)
for the reasoning and philosophy behind PyStore, as well as a detailed tutorial with code examples.

==> Follow [this PyStore tutorial](https://github.com/ranaroussi/pystore/blob/master/examples/pystore-tutorial.ipynb) in Jupyter notebook format.

## Quickstart

### Install PyStore

Install using `pip`:

```bash
$ pip install pystore --upgrade --no-cache-dir
```

Install using `conda`:

```bash
$ conda install -c ranaroussi pystore
```

**INSTALLATION NOTE:**
If you don't have Snappy installed (compression/decompression library), you'll need to
you'll need to [install it first](https://github.com/ranaroussi/pystore#dependencies).

### Using PyStore

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pystore
import quandl

# Set storage path (optional)
# Defaults to `~/pystore` or `PYSTORE_PATH` environment variable (if set)
pystore.set_path("~/pystore")

# List stores
pystore.list_stores()

# Connect to datastore (create it if not exist)
store = pystore.store('mydatastore')

# List existing collections
store.list_collections()

# Access a collection (create it if not exist)
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


# --- Query functionality ---

# Query avaialable symbols based on metadata
collection.list_items(some_key='some_value', other_key='other_value')


# --- Snapshot functionality ---

# Snapshot a collection
# (Point-in-time named reference for all current symbols in a collection)
collection.create_snapshot('snapshot_name')

# List available snapshots
collection.list_snapshots()

# Get a version of a symbol given a snapshot name
collection.item('AAPL', snapshot='snapshot_name')

# Delete a collection snapshot
collection.delete_snapshot('snapshot_name')


# ...


# Delete the item from the current version
collection.delete_item('AAPL')

# Delete the collection
store.delete_collection('NASDAQ')
```

### Using Dask schedulers

PyStore 0.1.18+ supports using Dask distributed.

To use a local Dask scheduler, add this to your code:

```python
from dask.distributed import LocalCluster
pystore.set_client(LocalCluster())
```

To use a distributed Dask scheduler, add this to your code:

```python
pystore.set_client("tcp://xxx.xxx.xxx.xxx:xxxx")
pystore.set_path("/path/to/shared/volume/all/workers/can/access")
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

* Python 2.7 or Python > 3.5
* Pandas
* Numpy
* Dask
* Fastparquet
* [Snappy](http://google.github.io/snappy/) (Google's compression/decompression library)
* multitasking

PyStore was tested to work on \*nix-like systems, including macOS.

### Dependencies

PyStore uses [Snappy](http://google.github.io/snappy/),
a fast and efficient compression/decompression library from Google.
You'll need to install Snappy on your system before installing PyStore.

\* See the `python-snappy` [Github repo](https://github.com/andrix/python-snappy#dependencies) for more information.

**\*nix Systems:**

- APT: `sudo apt-get install libsnappy-dev`
- RPM: `sudo yum install libsnappy-devel`

**macOS:**

First, install Snappy's C library using [Homebrew](https://brew.sh):

```bash
$ brew install snappy
```

Then, install Python's snappy using conda:

```bash
$ conda install python-snappy -c conda-forge
```

...or, using `pip`:

```bash
$ CPPFLAGS="-I/usr/local/include -L/usr/local/lib" pip install python-snappy
```

**Windows:**

Windows users should checkout [Snappy for Windows](https://snappy.machinezoo.com) and [this Stackoverflow post](https://stackoverflow.com/a/43756412/1783569) for help on installing Snappy and `python-snappy`.

## Roadmap

PyStore currently offers support for local filesystem (including attached network drives).
I plan on adding support for Amazon S3 (via [s3fs](http://s3fs.readthedocs.io)),
Google Cloud Storage (via [gcsfs](https://github.com/dask/gcsfs))
and Hadoop Distributed File System (via [hdfs3](http://hdfs3.readthedocs.io)) in the future.

## Acknowledgements

PyStore is hugely inspired by [Man AHL](http://www.ahl.com/)'s
[Arctic](https://github.com/manahl/arctic) which uses
MongoDB for storage and allow for versioning and other features.
I highly reommend you check it out.

## License

PyStore is licensed under the **Apache License, Version 2.0**. A copy of which is included in LICENSE.txt.

---

I'm very interested in your experience with PyStore.
Please drop me an note with any feedback you have.

Contributions welcome!

\- **Ran Aroussi**
