Change Log
===========

0.1.22
------
- Uses `PYSTORE_PATH` environment variable, if it exists, as the path when not calling `store.set_path()` (defaults to `~/pystore`)

0.1.21
------
- Updated PyPi install script (lib is the same as 0.1.20)

0.1.20
------
- Fix: Resetting `config._CLIENT` to `None`

0.1.19
------
- Fixed: Exposed set/get_partition_size and set/get_clients

0.1.18
------
- Added support for `dask.distributed` via `pystore.set_client(...)`
- Added `store.item(...)` for accessing single collection item directly (pull request #44)
- Added `store.set_partition_size(...)` and `store.get_partition_size()`. Default is ~99MB.

0.1.17
------
- Updated PyPi install script (lib is the same as 0.1.16)

0.1.16
------
- Fixed `npartition=None` issues on `.append()`

0.1.15
------
- Fixed append issues
- Raising an error when trying to read invalid item
- Fixed path issued (removed unnecessary os.path.join calls)

0.1.14
------
- Auto-detection and handling of nano-second based data

0.1.13
------
- `collection.reload_items` defaults to `False`
- Default `npartitions` and `chunksize` are better optimized (~99MB/partition)
- `collection.apply()` repartitions the dataframe based on new data size (~99MB/partition)
- Option to specify the default engine for the store by specifying `engine="fastparquet"` or `engine="pyarrow"` (dafaults to `fastparquet`)
- Solving `fastparquet`/`numba` issues when using Dask >= 2.2.0 by importing `numba` in `__init__.py`

0.1.12
------
- Added `reload_items` (default `True`) to `collection.write` and `collection.delete` to explicitly re-read the collection's items' directory

0.1.11
------
- Reversed `list_snapshots()` behaviour
- Added `collection.threaded_write(...)` method
- `collection.items` being updated using `items.add()` and an async/threaded directory read

0.1.10
------
- Switched from `dtype_str` to `str(dtype)` (Pandas 0.25+ compatibility)
- Implemented `collection.items` and `collection.snapshots` as `@property` to reduce initialization overhead
- `collection.items` and `collection.snapshots` are now of type `set()`
- Option to specify both `npartitions` and `chunksize` in `collection.append()`

0.1.9
------
- Fixed issues #13 and #15

0.1.8
------
- Added `pystore.read-csv()` to quickly load csv as dask dataframe, ready for storage

0.1.7
------
- Using `os.path.expanduser("~")` to determine user's home directory
- `collection.write(...)` accepts Dask dataframes

0.1.6
------
- Misc improvements

0.1.5
------

- Added support for Python 2.7

0.1.4
------

- Added support for Python 3.7

0.1.3
------

- Fixed support for nanosecond-level data

0.1.2
------

- `epochdate` defaults to `True` when storing ns data
- Switched to `dtype_str` instead of `str(dtype)`

0.1.1
------

- Infer datetime format when converting to Pandas

0.1.0
------

- Increased version to fix setup
- Bugfixes

0.0.12
------

- Switched path parsing to `pathlib.Path` to help with cross-platform compatibility
- Minor code refactoring

0.0.11
------

-  Adding an index name when one is not available

0.0.10
------

- Added `pystore.delete_store(NAME)`, `pystore.delete_stores()`, and `pystore.get_path()`
- Added Jupyter notebook example to Github repo
- Minor code refactoring

0.0.9
-----

- Allowing _ and . in snapshot name

0.0.8
-----

- Changed license to Apache License, Version 2.0
- Moduled seperated into files
- Code refactoring

0.0.7
-----

- Added support for snapshots
- `collection.list_items()` supports querying based on metadata
- Some code refactoring

-----

- Exposing more methods
- Path setting moved to `pystore.set_path()`
- `Store.collection()` auto-creates collection
- Updated readme to reflect changes
- Minor code refactoring


0.0.5
-----

- Not converting datetimte to epoch by defaults (use `epochdate=True` to enable)
- Using "snappy" compression by default
- Metadata's "_updated" is now a `YYYY-MM-DD HH:MM:SS.MS` string

0.0.4
-----

* Can pass columns and filters to Item object
* Faster append
* `Store.path` is now public

0.0.3
-----

* Updated license version

0.0.2
-----

* Switched readme/changelog files from `.md` to `.rst`.

0.0.1
-----

* Initial release
