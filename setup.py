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

import codecs
from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with codecs.open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='PyStore',
    version="0.1.23",
    description='Fast data store for Pandas timeseries data',
    long_description=long_description,
    url='https://github.com/ranaroussi/pystore',
    author='Ran Aroussi',
    author_email='ran@aroussi.com',
    license='Apache Software License',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        # 'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',

        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    platforms=['linux', 'unix', 'macOS'],
    keywords='dask, datastore, flatfile, pystore',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['python-snappy', 'multitasking', 'toolz', 'partd',
                      'cloudpickle', 'distributed', 'pandas', 'numpy',
                      'fastparquet', 'dask'],
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
