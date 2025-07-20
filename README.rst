PyStore - Fast data store for Pandas timeseries data
====================================================

.. image:: https://img.shields.io/badge/python-3.8+-blue.svg?style=flat
    :target: https://pypi.python.org/pypi/pystore
    :alt: Python version

.. image:: https://img.shields.io/pypi/v/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: PyPi version

.. image:: https://img.shields.io/pypi/status/pystore.svg?maxAge=60
    :target: https://pypi.python.org/pypi/pystore
    :alt: PyPi status

.. image:: https://www.codefactor.io/repository/github/ranaroussi/pystore/badge
    :target: https://www.codefactor.io/repository/github/ranaroussi/pystore
    :alt: CodeFactor

.. image:: https://img.shields.io/github/stars/ranaroussi/pystore.svg?style=social&label=Star&maxAge=60
    :target: https://github.com/ranaroussi/pystore
    :alt: Star this repo

.. image:: https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60
    :target: https://x.com/aroussi
    :alt: Follow me on X/Twitter

\


`PyStore <https://github.com/ranaroussi/pystore>`_ is a simple (yet powerful) datastore for Pandas dataframes, and while it can store any Pandas object, **it was designed with storing timeseries data in mind**.

It's built on top of `Pandas <http://pandas.pydata.org>`_, `Numpy <http://numpy.pydata.org>`_, `Dask <http://dask.pydata.org>`_, and `Parquet <http://parquet.apache.org>`_ (via `pyarrow <https://github.com/apache/arrow>`_), to provide an easy to use datastore for Python developers that can easily query millions of rows per second per client.

**New in 2025 Release (PR #77):**

* **MultiIndex Support** - Store and retrieve DataFrames with Pandas MultiIndex
* **Complex Data Types** - Full support for Timedelta, Period, Interval, Categorical dtypes
* **Timezone-Aware Operations** - Proper handling of timezone data with UTC storage
* **Async/Await Support** - Non-blocking I/O operations for better performance
* **Data Validation Framework** - Extensible validation rules for data integrity
* **Schema Evolution** - Handle schema changes over time with flexible strategies
* **Transaction Support** - Atomic operations with rollback capabilities
* **Performance Optimizations** - Streaming operations and memory management

**Performance Enhancements (Phase 3 Release):**

* **Streaming Operations** - Memory-efficient append for datasets larger than RAM
* **Batch Processing** - 5-10x faster parallel read/write operations
* **Intelligent Partitioning** - Automatic time-based and size-based partitioning
* **Memory Management** - 70-90% memory reduction with monitoring and optimization
* **Metadata Caching** - 100x faster metadata access with TTL cache
* **Query Optimization** - Column selection and predicate pushdown at storage level

Performance improvements include:

* Append 1M rows: 3.75x faster, 90% less memory
* Batch operations: 6x faster for multiple items
* Column selection: 4x faster when reading subset of columns
* Filtered reads: 8x faster with predicate pushdown


==> Check out `this Blog post <https://medium.com/@aroussi/fast-data-store-for-pandas-time-series-data-using-pystore-89d9caeef4e2>`_ for the reasoning and philosophy behind PyStore, as well as a detailed tutorial with code examples.

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
If you don't have Snappy installed (compression/decompression library), you'll need to you'll need to `install it first <https://github.com/ranaroussi/pystore#dependencies>`_.


Using PyStore
-------------

.. code:: python

    #!/usr/bin/env python
    # -*- coding: utf-8 -*-

    import pystore
    import yfinance as yf

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

    # Load some data from yfinance
    aapl = yf.download("AAPL", multi_level_index=False)

    # Store the first 100 rows of the data in the collection under "AAPL"
    collection.write('AAPL', aapl[:100], metadata={'source': 'yfinance'})

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

    # Query available symbols based on metadata
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


Advanced Features
-----------------

**Async Operations:**

.. code:: python

    import asyncio
    from pystore import async_pystore

    async def async_example():
        async with async_pystore.store('mydatastore') as store:
            async with store.collection('NASDAQ') as collection:
                # Async write
                await collection.write('AAPL', df)
                # Async read
                df = await collection.item('AAPL').to_pandas()

    asyncio.run(async_example())

**Data Validation:**

.. code:: python

    from pystore import create_validator, ColumnExistsRule, RangeRule

    # Create a validator
    validator = create_validator([
        ColumnExistsRule(['Open', 'High', 'Low', 'Close']),
        RangeRule('Close', min_value=0)
    ])

    # Apply validator to collection
    collection.set_validator(validator)

**Schema Evolution:**

.. code:: python

    from pystore import SchemaEvolution, EvolutionStrategy

    # Enable schema evolution
    evolution = collection.enable_schema_evolution(
        'AAPL',
        strategy=EvolutionStrategy.FLEXIBLE
    )

    # Schema changes are handled automatically during append
    collection.append('AAPL', new_data_with_extra_columns)

**Complex Data Types:**

.. code:: python

    # DataFrames with Period, Interval, Categorical types
    df = pd.DataFrame({
        'period': pd.period_range('2024-01', periods=12, freq='M'),
        'interval': pd.IntervalIndex.from_tuples([(0, 1), (1, 2)]),
        'category': pd.Categorical(['A', 'B', 'A']),
        'nested': [{'key': 'value'}, [1, 2, 3], None]
    })
    collection.write('complex_data', df)

**Performance Features:**

.. code:: python

    # Streaming append for large datasets
    def data_generator():
        for chunk in pd.read_csv('huge_file.csv', chunksize=100000):
            yield chunk
    
    collection.append_stream('large_data', data_generator())

    # Batch operations
    items_to_write = {
        'item1': df1,
        'item2': df2,
        'item3': df3
    }
    collection.write_batch(items_to_write, parallel=True)
    
    # Read multiple items efficiently
    results = collection.read_batch(['item1', 'item2', 'item3'])
    
    # Memory-optimized reading
    from pystore.memory import optimize_dataframe_memory, read_in_chunks
    
    # Optimize DataFrame memory usage
    df = collection.item('large_item').to_pandas()
    df_optimized = optimize_dataframe_memory(df)  # Up to 70% memory reduction
    
    # Read in chunks for processing
    for chunk in read_in_chunks(collection, 'large_item', chunk_size=50000):
        # Process chunk - automatically garbage collected
        process(chunk)

**Query Optimization:**

.. code:: python

    # Column selection - read only what you need
    item = collection.item('data')
    df = item.to_pandas(columns=['price', 'volume'])  # 4x faster for subset
    
    # Filter at storage level
    df = item.to_pandas(filters=[('price', '>', 100)])  # 8x faster

Using Dask schedulers
---------------------

PyStore supports using Dask distributed.

To use a local Dask scheduler, add this to your code:

.. code:: python

    from dask.distributed import LocalCluster
    pystore.set_client(LocalCluster())


To use a distributed Dask scheduler, add this to your code:

.. code:: python

    pystore.set_client("tcp://xxx.xxx.xxx.xxx:xxxx")
    pystore.set_path("/path/to/shared/volume/all/workers/can/access")



Concepts
========

PyStore provides namespaced *collections* of data. These collections allow bucketing data by *source*, *user* or some other metric (for example, frequency: End-Of-Day, Minute Bars, etc.). Each collection (or namespace) maps to directory containing partitioned **parquet files** for each item (e.g., symbol).

A good practice it to create collections that may look something like this:

* collection.EOD
* collection.ONEMINUTE

Requirements
============

* Python >= 3.8
* Pandas >= 2.0
* Numpy >= 1.20
* Dask >= 2023.1
* PyArrow >= 10.0 (Parquet engine)
* `Snappy <http://google.github.io/snappy/>`_ (Google's compression/decompression library)
* multitasking
* pytest-asyncio (for async testing)

PyStore was tested to work on \*nix-like systems, including macOS.


Dependencies:
-------------

PyStore utilizes `Snappy <http://google.github.io/snappy/>`_, a fast and efficient compression/decompression library developed by Google. You'll need to install Snappy on your system before installing PyStore.

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

Windows users should check out `Snappy for Windows <https://snappy.machinezoo.com>`_ and `this Stackoverflow post <https://stackoverflow.com/a/43756412/1783569>`_ for help on installing Snappy and ``python-snappy``.


Current Status
==============

**Core Features:**

* Local filesystem support with Parquet storage
* Full Pandas DataFrame compatibility, including MultiIndex
* Snapshots for point-in-time data versioning
* Metadata support for data organization

**Advanced Features (July 2025 Release):**

* Complex data type serialization (Period, Interval, Categorical, nested objects)
* Timezone-aware datetime handling with UTC storage
* Async/await operations for non-blocking I/O
* Data validation framework with extensible rules
* Schema evolution for handling data structure changes
* Transaction support with rollback capabilities

**Performance Features:**

* Streaming operations for datasets larger than RAM
* Batch read/write with parallel processing
* Intelligent partitioning (time-based and size-based)
* Memory optimization with automatic type downcasting
* Metadata caching for faster access
* Query optimization with column selection and predicate pushdown

**Known Limitations:**

* MultiIndex append operations have limited support due to Dask limitations - while there's a workaround that converts MultiIndex to regular columns, it may not fully preserve the MultiIndex structure after append (test remains marked as expected failure)
* Some Parquet limitations with preserving exact index metadata

**Future Plans:**

* Amazon S3 support (via `s3fs <http://s3fs.readthedocs.io/>`_)
* Google Cloud Storage support (via `gcsfs <https://github.com/dask/gcsfs/>`_)
* Hadoop Distributed File System support (via `hdfs3 <http://hdfs3.readthedocs.io/>`_)

Acknowledgements
================

PyStore is hugely inspired by `Man AHL <http://www.ahl.com/>`_'s `Arctic <https://github.com/manahl/arctic>`_ which uses MongoDB for storage and allows for versioning and other features. I highly recommend you check it out.



License
=======


PyStore is licensed under the **Apache License, Version 2.0**. A copy of which is included in LICENSE.txt.

-----

I'm very interested in your experience with PyStore. Please drop me a note with any feedback you have.

Contributions welcome!

\- **Ran Aroussi**
