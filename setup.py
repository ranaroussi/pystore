#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018 Ran Aroussi
#

"""
PyStore is a Pythonic, flat-file datastore for timeseries data,
that provide an easy to use datastore for Python developers that
can easily query millions of rows per second per client.
"""

import codecs
from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with codecs.open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='PyStore',
    version="0.0.2",
    description='Flat-file datastore for timeseries data',
    long_description=long_description,
    url='https://github.com/ranaroussi/pystore',
    author='Ran Aroussi',
    author_email='ran@aroussi.com',
    license='LGPL',
    classifiers=[
        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
        'Development Status :: 3 - Alpha',

        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    platforms=['linux', 'unix', 'macOS'],
    keywords='dask, datastore, flatfile, pystore',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['pandas', 'numpy', 'fastparquet', 'dask', 'toolz', 'partd', 'cloudpickle', 'distributed'],
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
