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
from flask import Flask, jsonify, request
from flask_cors import CORS

import pystore
from pystore.utils import get_path


def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Enable CORS for all routes
    CORS(app)
    
    @app.route('/health', methods=['GET'])
    def health():
        """Health check endpoint."""
        return jsonify({'status': 'healthy'}), 200
    
    @app.route('/index', methods=['GET'])
    def index():
        """Index endpoint that returns store information."""
        store_path = str(get_path())
        stores = pystore.list_stores()
        return jsonify({
            'store_path': store_path,
            'stores': stores
        }), 200
    
    @app.route('/stores', methods=['GET'])
    def list_stores():
        """List all available stores."""
        stores = pystore.list_stores()
        return jsonify({'stores': stores}), 200
    
    @app.route('/stores/<store_name>', methods=['GET'])
    def get_store(store_name):
        """Get information about a specific store."""
        try:
            store = pystore.store(store_name)
            collections = store.list_collections()
            return jsonify({
                'store': store_name,
                'collections': collections
            }), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 404
    
    @app.route('/stores/<store_name>/collections/<collection_name>', methods=['GET'])
    def get_collection(store_name, collection_name):
        """Get information about a specific collection."""
        try:
            store = pystore.store(store_name)
            collection = store.collection(collection_name)
            items = collection.list_items()
            return jsonify({
                'store': store_name,
                'collection': collection_name,
                'items': items
            }), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 404
    
    @app.route('/stores/<store_name>/collections/<collection_name>/items/<item_name>', methods=['GET'])
    def get_item(store_name, collection_name, item_name):
        """Get a specific item from a collection."""
        try:
            store = pystore.store(store_name)
            collection = store.collection(collection_name)
            item = collection.item(item_name)
            
            # Convert to pandas and then to dict for JSON serialization
            df = item.to_pandas()
            data = df.to_dict(orient='records')
            metadata = item.metadata
            
            return jsonify({
                'store': store_name,
                'collection': collection_name,
                'item': item_name,
                'data': data,
                'metadata': metadata
            }), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 404
    
    return app


def run(host='0.0.0.0', port=5000, debug=False):
    """Run the Flask server."""
    app = create_app()
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run()
