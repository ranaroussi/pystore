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

"""Tests for the PyStore server with logging middleware"""

import pytest
import logging


# Import the server components
from pystore.server import create_app, LoggingMiddleware


@pytest.fixture(autouse=True)
def setup_logging():
    """Ensure logging is configured for tests"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    yield


class TestLoggingMiddleware:
    """Test cases for the LoggingMiddleware class"""
    
    def test_middleware_initialization(self):
        """Test that middleware can be initialized with a Flask app"""
        app = create_app()
        assert app is not None
    
    def test_health_endpoint(self):
        """Test that health endpoint returns correct response"""
        app = create_app()
        client = app.test_client()
        response = client.get('/health')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['status'] == 'ok'
    
    def test_index_endpoint(self):
        """Test that index endpoint returns correct response"""
        app = create_app()
        client = app.test_client()
        response = client.get('/')
        
        assert response.status_code == 200
        data = response.get_json()
        assert 'message' in data
    
    def test_logging_captures_request_method(self, caplog):
        """Test that logging captures the request method"""
        caplog.set_level(logging.INFO)
        app = create_app()
        client = app.test_client()
        
        # Make a request
        client.get('/health')
        
        # Check log contains method
        assert 'GET' in caplog.text
    
    def test_logging_captures_status_code(self, caplog):
        """Test that logging captures the response status code"""
        caplog.set_level(logging.INFO)
        app = create_app()
        client = app.test_client()
        
        # Make a request
        client.get('/health')
        
        # Check log contains status code
        assert 'status=200' in caplog.text
    
    def test_logging_captures_path(self, caplog):
        """Test that logging captures the request path"""
        caplog.set_level(logging.INFO)
        app = create_app()
        client = app.test_client()
        
        # Make a request
        client.get('/health')
        
        # Check log contains path
        assert '/health' in caplog.text
    
    def test_logging_captures_duration(self, caplog):
        """Test that logging captures request duration"""
        caplog.set_level(logging.INFO)
        app = create_app()
        client = app.test_client()
        
        # Make a request
        client.get('/health')
        
        # Check log contains duration
        assert 'duration=' in caplog.text
    
    def test_logging_format(self, caplog):
        """Test that logging output is properly formatted"""
        caplog.set_level(logging.INFO)
        app = create_app()
        client = app.test_client()
        
        # Make a request
        client.get('/health')
        
        # Check log format - should contain method, path, status, duration
        assert 'GET' in caplog.text
        assert '/health' in caplog.text
        assert 'status=200' in caplog.text
        assert 'duration=' in caplog.text


class TestServer:
    """Test cases for the Flask server"""
    
    def test_app_creation(self):
        """Test that Flask app can be created"""
        app = create_app()
        assert app is not None
    
    def test_app_has_health_endpoint(self):
        """Test that app has health endpoint"""
        app = create_app()
        assert '/health' in [rule.rule for rule in app.url_map.iter_rules()]
    
    def test_app_has_index_endpoint(self):
        """Test that app has index endpoint"""
        app = create_app()
        assert '/' in [rule.rule for rule in app.url_map.iter_rules()]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
