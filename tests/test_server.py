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

"""Tests for PyStore API Server"""

import unittest
import json
import tempfile
import shutil
import os

import pystore
from pystore.server import create_app


class TestServer(unittest.TestCase):
    """Test cases for the PyStore API server."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for pystore
        self.temp_dir = tempfile.mkdtemp()
        pystore.set_path(self.temp_dir)
        
        # Create a test store
        self.store = pystore.store('test_store')
        
        # Create the Flask test client
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up the temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_app_creation(self):
        """Test that the Flask app is created correctly."""
        app = create_app()
        self.assertIsNotNone(app)

    def test_health_endpoint(self):
        """Test the /health endpoint."""
        response = self.client.get('/health')
        data = json.loads(response.data)
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'healthy')

    def test_index_endpoint(self):
        """Test the / endpoint."""
        response = self.client.get('/')
        data = json.loads(response.data)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('name', data)
        self.assertIn('description', data)
        self.assertIn('version', data)
        self.assertEqual(data['name'], 'PyStore API')

    def test_list_stores_endpoint(self):
        """Test the /stores endpoint."""
        response = self.client.get('/stores')
        data = json.loads(response.data)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('stores', data)
        self.assertIn('test_store', data['stores'])

    def test_cors_headers_health(self):
        """Test CORS headers on health endpoint."""
        response = self.client.get('/health')
        
        self.assertIn('Access-Control-Allow-Origin', response.headers)

    def test_cors_headers_index(self):
        """Test CORS headers on index endpoint."""
        response = self.client.get('/')
        
        self.assertIn('Access-Control-Allow-Origin', response.headers)

    def test_cors_preflight_request(self):
        """Test CORS preflight request."""
        response = self.client.options(
            '/',
            headers={'Access-Control-Request-Method': 'GET', 'Origin': 'http://example.com'}
        )
        
        self.assertEqual(response.status_code, 200)

    def test_version_endpoint(self):
        """Test the /version endpoint."""
        response = self.client.get('/version')
        data = json.loads(response.data)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('version', data)
        self.assertEqual(data['version'], pystore.__version__)


if __name__ == '__main__':
    unittest.main()
