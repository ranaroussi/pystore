#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018 Ran Aroussi
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


class store(object):
    def __repr__(self):
        return 'PyStore.datastore <%s>' % self.datastore

    def __init__(self, datastore):

        if not os.path.exists(utils.get_path()):
            os.makedirs(utils.get_path())

        self.datastore = utils.get_path() + '/' + datastore  # <-- this is just a diretory
        if not os.path.exists(self.datastore):
            os.makedirs(self.datastore)

        self.collections = self.list_collections()

    def _create_collection(self, collection, overwrite=False):
        # create collection (subdir)
        if os.path.exists(self.datastore + '/' + collection):
            if overwrite:
                self.delete_collection(collection)
            else:
                raise ValueError(
                    "Collection already exists. To overwrite, use `overwrite=True`")

        os.makedirs(self.datastore + '/' + collection)
        os.makedirs(self.datastore + '/' + collection + '/_snapshots')

        # update collections
        self.collections = self.list_collections()

        # return the collection
        return Collection(collection, self.datastore)

    def delete_collection(self, collection):
        # delete collection (subdir)
        shutil.rmtree(self.datastore + '/' + collection)

        # update collections
        self.collections = self.list_collections()
        return True

    def list_collections(self):
        # lists collections (subdirs)
        return utils.subdirs(self.datastore)

    def collection(self, collection, overwrite=False):
        if collection in self.collections and not overwrite:
            return Collection(collection, self.datastore)

        # create it
        self._create_collection(collection, overwrite)
        return Collection(collection, self.datastore)
