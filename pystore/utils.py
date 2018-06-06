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
import json
import shutil
import pandas as pd
import numpy as np

PATH = '~/.pystore'


def datetime_to_int64(df):
    """ convert datetime index to epoch int
    allows for cross language/platform portability
    """
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.astype(np.int64) / 1e9
    return df


def subdirs(d):
    return [os.path.join(d, o).replace(d + '/', '') for o in os.listdir(d)
            if (os.path.isdir(os.path.join(d, o)) and o != '_snapshots')]


def read_metadata(path):
    with open(path + '/metadata.json') as f:
        return json.load(f)


def set_path(path):
    global PATH

    if path is None:
        path = '~/.pystore'

    path = path.rstrip('/').rstrip('\\').rstrip(' ')
    if "://" in path and "file://" not in path:
        raise ValueError(
            "PyStore currently only works with local file system")

    # if path ot exist - create it
    PATH = path
    if not os.path.exists(PATH):
        os.makedirs(PATH)

    return PATH


def list_stores():
    global PATH

    if not os.path.exists(PATH):
        os.makedirs(PATH)

    return subdirs(PATH)

def delete_store(store):
    global PATH
    shutil.rmtree(PATH + '/' + store)
    return True

def delete_stores():
    global PATH
    shutil.rmtree(PATH)
    return True
