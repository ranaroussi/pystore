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

"""Flask server for PyStore with logging middleware"""

import logging
import time
from flask import Flask, jsonify, request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get logger - use root logger to ensure logging captures work
logger = logging.getLogger()


class LoggingMiddleware:
    """Flask extension for logging HTTP request/response details"""
    
    def __init__(self, app=None):
        self.app = app
        self.request_start_time = None
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize the middleware with a Flask app"""
        app.before_request(self.before_request)
        app.after_request(self.after_request)
    
    def before_request(self):
        """Record the start time of the request"""
        self.request_start_time = time.time()
    
    def after_request(self, response):
        """Log request details after the response is generated"""
        if self.request_start_time is not None:
            duration = time.time() - self.request_start_time
            
            # Get client IP address
            client_ip = request.remote_addr or 'unknown'
            
            # Log the request details
            logger.info(
                f"{request.method} {request.path} "
                f"status={response.status_code} "
                f"duration={duration:.4f}s "
                f"client={client_ip}"
            )
            
            self.request_start_time = None
        
        return response


def create_app():
    """Create and configure the Flask application"""
    app = Flask(__name__)
    
    # Initialize logging middleware
    LoggingMiddleware(app)
    
    @app.route('/health')
    def health():
        """Health check endpoint"""
        return jsonify({'status': 'ok'})
    
    @app.route('/')
    def index():
        """Index endpoint"""
        return jsonify({'message': 'PyStore API'})
    
    return app


# Default app instance
app = create_app()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
