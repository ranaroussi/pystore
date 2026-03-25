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

"""PyStore CLI - Command Line Interface for PyStore"""

import argparse
import sys
import os

from . import __version__
from . import store, list_stores, delete_store, set_path, get_path


def main():
    """Main entry point for the pystore CLI."""
    parser = argparse.ArgumentParser(
        prog='pystore',
        description='PyStore - Fast data store for Pandas timeseries data',
        epilog='For more information, visit https://github.com/ranaroussi/pystore'
    )
    
    parser.add_argument(
        '--version', 
        action='version', 
        version=f'%(prog)s {__version__}'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List stores command
    list_parser = subparsers.add_parser('list', help='List all available stores')
    list_parser.add_argument(
        '--path', 
        help='Path to the pystore directory (default: ~/pystore or PYSTORE_PATH)'
    )
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show PyStore information')
    info_parser.add_argument(
        '--path', 
        help='Path to the pystore directory (default: ~/pystore or PYSTORE_PATH)'
    )
    
    # Delete store command
    delete_parser = subparsers.add_parser('delete', help='Delete a store')
    delete_parser.add_argument('store_name', help='Name of the store to delete')
    delete_parser.add_argument(
        '--path', 
        help='Path to the pystore directory (default: ~/pystore or PYSTORE_PATH)'
    )
    delete_parser.add_argument(
        '--force', 
        action='store_true', 
        help='Force deletion without confirmation'
    )
    
    args = parser.parse_args()
    
    # Handle no arguments - show help
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    
    # Handle list command
    if args.command == 'list':
        if args.path:
            set_path(args.path)
        stores = list_stores()
        if stores:
            print("Available stores:")
            for s in stores:
                print(f"  - {s}")
        else:
            print("No stores found.")
        return 0
    
    # Handle info command
    if args.command == 'info':
        if args.path:
            set_path(args.path)
        print(f"PyStore version: {__version__}")
        print(f"Storage path: {get_path()}")
        stores = list_stores()
        print(f"Number of stores: {len(stores)}")
        return 0
    
    # Handle delete command
    if args.command == 'delete':
        if args.path:
            set_path(args.path)
        
        if not args.force:
            confirm = input(f"Are you sure you want to delete store '{args.store_name}'? [y/N]: ")
            if confirm.lower() not in ('y', 'yes'):
                print("Deletion cancelled.")
                return 0
        
        try:
            delete_store(args.store_name)
            print(f"Store '{args.store_name}' deleted successfully.")
        except Exception as e:
            print(f"Error: {e}")
            return 1
        return 0
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
