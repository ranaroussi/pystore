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
import tempfile
import unittest

try:
    from pystore.server import create_app
except ImportError:
    # If flask-cors is not installed, we need to handle that
    import sys
    sys.exit("flask-cors is required. Install with: pip install flask-cors")


class TestServer(unittest.TestCase):
    """Test cases for the PyStore server."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for pystore data
        self.test_dir = tempfile.mkdtemp()
        os.environ['PYSTORE_PATH'] = self.test_dir
        
        # Create the Flask test client
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up the temporary directory
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        
        # Remove the environment variable
        if 'PYSTORE_PATH' in os.environ:
            del os.environ['PYSTORE_PATH']
    
    def test_app_creation(self):
        """Test that the Flask app is created correctly."""
        app = create_app()
        self.assertIsNotNone(app)
        self.assertEqual(app.name, 'pystore.server')
    
    def test_health_endpoint(self):
        """Test the health endpoint returns healthy status."""
        response = self.client.get('/health')
        self.assertEqual(response.status_code, 200)
        
        data = response.get_json()
        self.assertEqual(data['status'], 'healthy')
    
    def test_index_endpoint(self):
        """Test the index endpoint returns store information."""
        response = self.client.get('/index')
        self.assertEqual(response.status_code, 200)
        
        data = response.get_json()
        self.assertIn('store_path', data)
        self.assertIn('stores', data)
        self.assertIsInstance(data['stores'], list)
    
    def test_cors_headers_health(self):
        """Test that CORS headers are present in health endpoint response."""
        response = self.client.get('/health')
        # Check for CORS headers (flask-cors adds these automatically)
        # The Access-Control-Allow-Origin header should be present
        self.assertIn('Access-Control-Allow-Origin', response.headers)
    
    def test_cors_headers_index(self):
        """Test that CORS headers are present in index endpoint response."""
        response = self.client.get('/index')
        # Check for CORS headers
        self.assertIn('Access-Control-Allow-Origin', response.headers)
    
    def test_cors_preflight_request(self):
        """Test CORS preflight (OPTIONS) request."""
        response = self.client.options('/health')
        # Should return 200 for preflight
        self.assertEqual(response.status_code, 200)
        # Should have CORS headers
        self.assertIn('Access-Control-Allow-Origin', response.headers)
    
    def test_list_stores_endpoint(self):
        """Test the list stores endpoint."""
        response = self.client.get('/stores')
        self.assertEqual(response.status_code, 200)
        
        data = response.get_json()
        self.assertIn('stores', data)
        self.assertIsInstance(data['stores'], list)


if __name__ == '__main__':
    unittest.main()
