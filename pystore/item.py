#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
PyStore: Flat-file datastore for timeseries data
"""

import pandas as pd

from . import utils


class Item(object):
    def __repr__(self):
        return "PyStore.item <%s/%s>" % (self.collection, self.item)

    def __init__(
        self,
        item,
        datastore,
        collection,
        snapshot=None,
        filters=None,
        engine="fastparquet",
    ):
        self.engine = engine
        self.datastore = datastore
        self.collection = collection
        self.snapshot = snapshot
        self.item = item

        self._path = utils.make_path(datastore, collection, item, "data.parquet")
        self._metadata_path = utils.make_path(datastore, collection, item)

        if not self._path.exists():
            raise ValueError(
                "Item `%s` doesn't exist. "
                "Create it using collection.write(`%s`, data, ...)" % (item, item)
            )
        if snapshot:
            snap_path = utils.make_path(datastore, collection, "_snapshots", snapshot)

            self._path = utils.make_path(snap_path, item)

            if not utils.path_exists(snap_path):
                raise ValueError("Snapshot `%s` doesn't exist" % snapshot)

            if not utils.path_exists(self._path):
                raise ValueError("Item `%s` doesn't exist in this snapshot" % item)

        self.metadata = utils.read_metadata(self._metadata_path)
        self.data = pd.read_parquet(self._path, engine=self.engine, filters=filters)
