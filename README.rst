PyStore - Datastore for timeseries data
=======================================

.. image:: https://img.shields.io/pypi/pyversions/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: Python version

.. image:: https://img.shields.io/travis/ranaroussi/pystore/master.svg?maxAge=1
    :target: https://travis-ci.org/ranaroussi/pystore
    :alt: Travis-CI build status

.. image:: https://img.shields.io/pypi/v/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: PyPi version

.. image:: https://img.shields.io/pypi/status/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: PyPi status

.. image:: https://img.shields.io/github/stars/ranaroussi/pystore.svg?style=social&label=Star&maxAge=60
    :target: https://github.com/ranaroussi/pystore
    :alt: Star this repo

.. image:: https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow%20Me&maxAge=60
    :target: https://twitter.com/aroussi
    :alt: Follow me on twitter

\


`PyStore <https://github.com/ranaroussi/pystore>`_ is a simple (yet powerful)
datastore for timeseries data. It's built on top of
`Pandas <http://pandas.pydata.org>`_, `Numpy <http://numpy.pydata.org>`_,
`Dask <http://dask.pydata.org>`_, and `Parquet <http://parquet.apache.org>`_
(via `Fastparquet <https://github.com/dask/fastparquet>`_),
to provide an easy to use datastore for Python developers that can easily
query millions of rows per second per client.

PyStore is hugely inspired by `Man AHL <http://www.ahl.com/>`_'s
`Arctic <https://github.com/manahl/arctic>`_. I highly reommend you check it out.


Quickstart
==========

Install PyStore
---------------

Install using `pip`:

.. code:: bash

    $ pip install PyStore

Or upgrade using:

.. code:: bash

    $ pip install PyStore --upgrade --no-cache-dir


Using PyStore
-------------

.. code:: python

    #!/usr/bin/env python
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


Concepts
========

PyStore provides namespaced *collections* of data.
These collections allow bucketing data by *source*, *user* or some other metric
(for example frequency: End-Of-Day; Minute Bars; etc.). Each collection (or namespace)
maps to a directory containing partitioned **parquet files** for each item (e.g. symbol).

A good practice it to create collections that may look something like this:

* collection.EOD
* collection.ONEMINUTE

Known Limitation
================

PyStore currently only offers support for local filesystem.

I plan on adding support for Amazon S3 (via `s3fs <http://s3fs.readthedocs.io/>`_),
Google Cloud Storage (via `gcsfs <https://github.com/dask/gcsfs/>`_)
and Hadoop Distributed File System (via `hdfs3 <http://hdfs3.readthedocs.io/>`_) in the future.

Requirements
============

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


Acknowledgements
================

PyStore is hugely inspired by `Man AHL <http://www.ahl.com/>`_'s
`Arctic <https://github.com/manahl/arctic>`_ which uses
MongoDB for storage and allow for versioning and other features.
I highly reommend you check it out.

Contributions welcome!


License
=======

PyStore is licensed under the **GNU Lesser General Public License v2.1**. A copy of which is included in LICENSE.txt.

-----

I'm very interested in your experience with pystore. Please drop me an note with any feedback you have.

\- **Ran Aroussi**
