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

"""PyStore API Server"""

from flask import Flask, jsonify
from flask_cors import CORS

import pystore


def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    CORS(app)

    @app.route('/')
    def index():
        """Index endpoint - returns API information."""
        return jsonify({
            'name': 'PyStore API',
            'description': 'Fast data store for Pandas timeseries data',
            'version': pystore.__version__
        })

    @app.route('/health')
    def health():
        """Health check endpoint."""
        return jsonify({'status': 'healthy'})

    @app.route('/stores')
    def list_stores():
        """List all available stores."""
        stores = pystore.list_stores()
        return jsonify({'stores': stores})

    @app.route('/version')
    def version():
        """Version endpoint - returns PyStore version."""
        return jsonify({'version': pystore.__version__})

    return app


# Default app instance
app = create_app()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
