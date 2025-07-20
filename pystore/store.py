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

import os
import shutil

from . import utils
from .collection import Collection
from .exceptions import CollectionExistsError, CollectionNotFoundError
from .logger import get_logger

logger = get_logger(__name__)


class store(object):
    def __repr__(self):
        return "PyStore.datastore <%s>" % self.datastore

    def __init__(self, datastore):

        datastore_path = utils.get_path()
        if not utils.path_exists(datastore_path):
            os.makedirs(datastore_path)

        self.datastore = utils.make_path(datastore_path, datastore)
        if not utils.path_exists(self.datastore):
            os.makedirs(self.datastore)
            utils.write_metadata(self.datastore, {"engine": "pyarrow"})

        self.collections = self.list_collections()

    def _create_collection(self, collection, overwrite=False):
        # create collection (subdir)
        collection_path = utils.make_path(self.datastore, collection)
        if utils.path_exists(collection_path):
            if overwrite:
                self.delete_collection(collection)
            else:
                raise CollectionExistsError(
                    f"Collection '{collection}' already exists! To overwrite, use overwrite=True")

        os.makedirs(collection_path)
        os.makedirs(utils.make_path(collection_path, "_snapshots"))

        # update collections
        self.collections = self.list_collections()

        # return the collection
        return Collection(collection, self.datastore)

    def delete_collection(self, collection):
        # delete collection (subdir)
        collection_path = utils.make_path(self.datastore, collection)
        if not utils.path_exists(collection_path):
            raise CollectionNotFoundError(f"Collection '{collection}' does not exist")
        
        try:
            shutil.rmtree(collection_path)
            # update collections
            self.collections = self.list_collections()
            logger.info(f"Successfully deleted collection '{collection}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete collection '{collection}': {e}")
            raise RuntimeError(f"Failed to delete collection '{collection}': {str(e)}") from e

    def list_collections(self):
        # lists collections (subdirs)
        return utils.subdirs(self.datastore)

    def collection(self, collection, overwrite=False):
        if collection in self.collections and not overwrite:
            return Collection(collection, self.datastore)

        # create it
        self._create_collection(collection, overwrite)
        return Collection(collection, self.datastore)

    def item(self, collection, item):
        # bypasses collection
        return self.collection(collection).item(item)