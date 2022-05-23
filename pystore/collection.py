#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2020 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Union

import os
import time
import shutil
import multitasking
import pandas as pd

from . import utils
from .item import Item
from . import config
from .utils import make_path

Tensor = Union[pd.Series, pd.DataFrame]


class Collection(object):
    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore, engine="fastparquet"):
        self.engine = engine
        self.datastore = datastore
        self.collection = collection
        self.snapshots = self.list_snapshots()

    def _item_path(self, item, as_string=False):
        p = utils.make_path(self.datastore, self.collection, item)
        if as_string:
            return str(p)
        return p

    def list_items(self, **kwargs):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        if not kwargs:
            return set(dirs)

        matched = []
        for d in dirs:
            meta = utils.read_metadata(
                utils.make_path(self.datastore, self.collection, d)
            )
            del meta["_updated"]

            m = 0
            keys = list(meta.keys())
            for k, v in kwargs.items():
                if k in keys and meta[k] == v:
                    m += 1

            if m == len(kwargs):
                matched.append(d)

        return set(matched)

    def item(self, item, snapshot=None, filters=None):
        return Item(
            item, self.datastore, self.collection, snapshot, filters, engine=self.engine
        )

    def write(
        self,
        item,
        data: Tensor,
        metadata: dict = None,
        overwrite: bool = False,
    ):

        metadata = metadata or {}

        if utils.path_exists(self._item_path(item)) and not overwrite:
            raise ValueError(
                """
                Item already exists. To overwrite, use `overwrite=True`.
                Otherwise, use `<collection>.append()`"""
            )

        data_path = make_path(item, "data.parquet")
        data.to_parquet(
            self._item_path(data_path, as_string=True),
            compression="snappy",
            engine=self.engine,
        )

        metadata_path = make_path(item, "metadata.json")
        utils.write_metadata(
            utils.make_path(self.datastore, self.collection, metadata_path), metadata
        )

    def append(self, item, data):

        if not utils.path_exists(self._item_path(item)):
            raise ValueError("""Item do not exists. Use `<collection>.write(...)`""")

        current = pd.read_parquet(
            self._item_path(item, as_string=True), engine=self.engine
        )
        combined = current.append(data)

        current = self.item(item)

        self.write(
            item,
            combined,
            metadata=current.metadata,
            overwrite=True,
        )

    def create_snapshot(self, snapshot=None):
        if snapshot:
            snapshot = "".join(e for e in snapshot if e.isalnum() or e in [".", "_"])
        else:
            snapshot = str(int(time.time() * 1000000))

        src = utils.make_path(self.datastore, self.collection)
        dst = utils.make_path(src, "_snapshots", snapshot)

        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_snapshots"))

        self.snapshots = self.list_snapshots()
        return True

    def list_snapshots(self):
        snapshots = utils.subdirs(
            utils.make_path(self.datastore, self.collection, "_snapshots")
        )
        return set(snapshots)

    def delete_snapshot(self, snapshot):
        if snapshot not in self.snapshots:
            return True

        shutil.rmtree(
            utils.make_path(self.datastore, self.collection, "_snapshots", snapshot)
        )
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self):
        snapshots_path = utils.make_path(self.datastore, self.collection, "_snapshots")
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)
        self.snapshots = self.list_snapshots()
        return True
