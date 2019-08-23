PyStore - Fast data store for Pandas timeseries data
====================================================

.. image:: https://img.shields.io/badge/python-2.7,%203.5+-blue.svg?style=flat
    :target: https://pypi.python.org/pypi/pystore
    :alt: Python version

.. image:: https://img.shields.io/pypi/v/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: PyPi version

.. image:: https://img.shields.io/pypi/status/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: PyPi status

.. image:: https://img.shields.io/travis/ranaroussi/pystore/master.svg?maxAge=1
    :target: https://travis-ci.org/ranaroussi/pystore
    :alt: Travis-CI build status

.. image:: https://www.codefactor.io/repository/github/ranaroussi/pystore/badge
    :target: https://www.codefactor.io/repository/github/ranaroussi/pystore
    :alt: CodeFactor

.. image:: https://img.shields.io/github/stars/ranaroussi/pystore.svg?style=social&label=Star&maxAge=60
    :target: https://github.com/ranaroussi/pystore
    :alt: Star this repo

.. image:: https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60
    :target: https://twitter.com/aroussi
    :alt: Follow me on twitter

\


`PyStore <https://github.com/ranaroussi/pystore>`_ is a simple (yet powerful)
datastore for Pandas dataframes, and while it can store any Pandas object,
**it was designed with storing timeseries data in mind**.

It's built on top of `Pandas <http://pandas.pydata.org>`_, `Numpy <http://numpy.pydata.org>`_,
`Dask <http://dask.pydata.org>`_, and `Parquet <http://parquet.apache.org>`_
(via `Fastparquet <https://github.com/dask/fastparquet>`_),
to provide an easy to use datastore for Python developers that can easily
query millions of rows per second per client.


==> Check out `this Blog post <https://medium.com/@aroussi/fast-data-store-for-pandas-time-series-data-using-pystore-89d9caeef4e2>`_
for the reasoning and philosophy behind PyStore, as well as a detailed tutorial with code examples.

==> Follow `this PyStore tutorial <https://github.com/ranaroussi/pystore/blob/master/examples/pystore-tutorial.ipynb>`_ in Jupyter notebook format.


Quickstart
==========

Install PyStore
---------------

Install using `pip`:

.. code:: bash

    $ pip install pystore --upgrade --no-cache-dir

Install using `conda`:

.. code:: bash

    $ conda install -c ranaroussi pystore

**INSTALLATION NOTE:**
If you don't have Snappy installed (compression/decompression library), you'll need to
you'll need to `install it first <https://github.com/ranaroussi/pystore#dependencies>`_.


Using PyStore
-------------

.. code:: python

    #!/usr/bin/env python
    # -*- coding: utf-8 -*-

    import pystore
    import quandl

    # Set storage path (optional, default is `~/pystore`)
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


Concepts
========

PyStore provides namespaced *collections* of data.
These collections allow bucketing data by *source*, *user* or some other metric
(for example frequency: End-Of-Day; Minute Bars; etc.). Each collection (or namespace)
maps to a directory containing partitioned **parquet files** for each item (e.g. symbol).

A good practice it to create collections that may look something like this:

* collection.EOD
* collection.ONEMINUTE

Requirements
============

* Python >= 3.5
* Pandas
* Numpy
* Dask
* Fastparquet
* `Snappy <http://google.github.io/snappy/>`_ (Google's compression/decompression library)
* multitasking

PyStore was tested to work on \*nix-like systems, including macOS.


Dependencies:
-------------

PyStore uses `Snappy <http://google.github.io/snappy/>`_,
a fast and efficient compression/decompression library from Google.
You'll need to install Snappy on your system before installing PyStore.

\* See the ``python-snappy`` `Github repo <https://github.com/andrix/python-snappy#dependencies>`_ for more information.

***nix Systems:**

- APT: ``sudo apt-get install libsnappy-dev``
- RPM: ``sudo yum install libsnappy-devel``

**macOS:**

First, install Snappy's C library using `Homebrew <https://brew.sh>`_:

.. code::

    $ brew install snappy

Then, install Python's snappy using conda:

.. code::

    $ conda install python-snappy -c conda-forge

...or, using `pip`:

.. code::

    $ CPPFLAGS="-I/usr/local/include -L/usr/local/lib" pip install python-snappy


**Windows:**

Windows users should checkout `Snappy for Windows <https://snappy.machinezoo.com>`_ and `this Stackoverflow post <https://stackoverflow.com/a/43756412/1783569>`_ for help on installing Snappy and ``python-snappy``.


Roadmap
=======

PyStore currently offers support for local filesystem.
I plan on adding support for Amazon S3 (via `s3fs <http://s3fs.readthedocs.io/>`_),
Google Cloud Storage (via `gcsfs <https://github.com/dask/gcsfs/>`_)
and Hadoop Distributed File System (via `hdfs3 <http://hdfs3.readthedocs.io/>`_) in the future.

Acknowledgements
================

PyStore is hugely inspired by `Man AHL <http://www.ahl.com/>`_'s
`Arctic <https://github.com/manahl/arctic>`_ which uses
MongoDB for storage and allow for versioning and other features.
I highly reommend you check it out.



License
=======


PyStore is licensed under the **Apache License, Version 2.0**. A copy of which is included in LICENSE.txt.

-----

I'm very interested in your experience with PyStore.
Please drop me an note with any feedback you have.

Contributions welcome!

\- **Ran Aroussi**
