Change Log
===========

0.1.0
------

- Increased version to fix setup
- Bugfixes

0.0.12
------

- Switched path parsing to ``pathlib.Path`` to help with cross-platform compatibility
- Minor code refactoring

0.0.11
------

-  Adding an index name when one is not available

0.0.10
------

- Added ``pystore.delete_store(NAME)``, ``pystore.delete_stores()``, and ``pystore.get_path()``
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
- ``collection.list_items()`` supports querying based on metadata
- Some code refactoring

-----

- Exposing more methods
- Path setting moved to ``pystore.set_path()``
- ``Store.collection()`` auto-creates collection
- Updated readme to reflect changes
- Minor code refactoring


0.0.5
-----

- Not converting datetimte to epoch by defaults (use ``epochdate=True`` to enable)
- Using "snappy" compression by default
- Metadata's "_updated" is now a ``YYYY-MM-DD HH:MM:SS.MS`` string

0.0.4
-----

* Can pass columns and filters to Item object
* Faster append
* ``Store.path`` is now public

0.0.3
-----

* Updated license version

0.0.2
-----

* Switched readme/changelog files from ``.md`` to ``.rst``.

0.0.1
-----

* Initial release
