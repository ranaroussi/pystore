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

from . import config


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


def get_path():
    return config.DEFAULT_PATH


def set_path(path):
    if path is None:
        path = get_path()

    path = path.rstrip('/').rstrip('\\').rstrip(' ')
    if "://" in path and "file://" not in path:
        raise ValueError(
            "PyStore currently only works with local file system")

    config.DEFAULT_PATH = path
    path = get_path()

    # if path ot exist - create it
    if not os.path.exists(get_path()):
        os.makedirs(get_path())

    return get_path


def list_stores():
    if not os.path.exists(get_path()):
        os.makedirs(get_path())

    return subdirs(get_path())

def delete_store(store):
    shutil.rmtree(get_path() + '/' + store)
    return True

def delete_stores():
    shutil.rmtree(get_path())
    return True
