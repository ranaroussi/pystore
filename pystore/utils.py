#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2019 Ran Aroussi
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
from datetime import datetime
import json
import shutil
import pandas as pd
import numpy as np

try:
    from pathlib import Path
    Path().expanduser()
except (ImportError, AttributeError):
    from pathlib2 import Path

from . import config


def datetime_to_int64(df):
    """ convert datetime index to epoch int
    allows for cross language/platform portability
    """
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.astype(np.int64) #/ 1e9
    return df


def subdirs(d):
    """ use this to construct paths for future storage support """
    return [o.parts[-1] for o in Path(d).iterdir()
            if o.is_dir() and o.parts[-1] != '_snapshots']


def path_exists(path):
    """ use this to construct paths for future storage support """
    return path.exists()


def read_metadata(path):
    """ use this to construct paths for future storage support """
    with make_path(path, 'metadata.json').open() as f:
        return json.load(f)


def write_metadata(path, metadata={}):
    """ use this to construct paths for future storage support """
    now = datetime.now()
    metadata['_updated'] = now.strftime('%Y-%m-%d %H:%I:%S.%f')
    meta_file = make_path(path, 'metadata.json')
    with meta_file.open('w') as f:
        json.dump(metadata, f, ensure_ascii=False)


def make_path(*args):
    """ use this to construct paths for future storage support """
    return Path(os.path.join(*args))


def get_path(*args):
    """ use this to construct paths for future storage support """
    return Path(os.path.join(config.DEFAULT_PATH, *args))


def set_path(path):
    if path is None:
        path = get_path()

    else:
        path = path.rstrip('/').rstrip('\\').rstrip(' ')
        if "://" in path and "file://" not in path:
            raise ValueError(
                "PyStore currently only works with local file system")

    config.DEFAULT_PATH = path
    path = get_path()

    # if path ot exist - create it
    if not path_exists(get_path()):
        os.makedirs(get_path())

    return get_path


def list_stores():
    if not path_exists(get_path()):
        os.makedirs(get_path())
    return subdirs(get_path())


def delete_store(store):
    shutil.rmtree(get_path(store))
    return True


def delete_stores():
    shutil.rmtree(get_path())
    return True
