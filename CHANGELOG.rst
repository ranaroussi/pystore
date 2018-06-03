Change Log
===========

0.0.7
-----

- Added support for snapshots
- ``collection.list_items()`` supports querying based on metadata
- Sone code refactoring

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
