#!/usr/bin/env python3
# Secret Animal: Pig
"""
WeeWX Marine Data Extension - FUNCTIONAL Core Service

Copyright (C) 2025 Shane Burkhardt
"""

import json
import time
import threading
import urllib.request
import urllib.parse
import urllib.error
import socket
import os
import argparse
import sys
import math
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple

import weewx
from weewx.engine import StdService
from weewx.cheetahgenerator import SearchList
import weewx.units
import weewx.manager
import weeutil.logger
from weeutil.weeutil import to_bool

log = weeutil.logger.logging.getLogger(__name__)

VERSION = "1.0.1"

# CONSISTENT ICONS: Match install.py for consistency
CORE_ICONS = {
    'navigation': '📍',    # Location/station selection
    'status': '✅',        # Success indicators  
    'warning': '⚠️',       # Warnings/issues
    'selection': '🔧'      # Configuration/selection
}

class MarineDataAPIError(Exception):
    """Custom exception for marine data API errors"""
    
    def __init__(self, message, error_type=None, station_id=None, api_source=None):
        super().__init__(message)
        self.error_type = error_type
        self.station_id = station_id
        self.api_source = api_source


class MarineDataService(StdService):
    """
    FUNCTIONAL Marine Data Service - DATA DRIVEN by config_dict field_mappings
    
    Collects real marine data from NOAA APIs and stores in dedicated tables
    """
    
    def __init__(self, engine, config_dict):
        super(MarineDataService, self).__init__(engine, config_dict)
        
        log.info(f"Marine Data service version {VERSION} starting")
        
        self.engine = engine
        self.config_dict = config_dict
        
        # SUCCESS MANUAL PATTERN: Get service section from config_dict
        self.service_config = config_dict.get('MarineDataService', {})
        
        # Check if service is enabled
        enable = self.service_config.get('enable', 'true')
        if enable.lower() in ['false', 'no', '0']:
            log.info("Marine Data service disabled by configuration")
            self.service_enabled = False
            return
        
        # Initialize WeeWX database manager
        try:
            self.db_manager = engine.db_binder.get_manager('wx_binding')
            log.info("Marine Data service using WeeWX database manager")
        except Exception as e:
            log.error(f"Error initializing database manager: {e}")
            self.service_enabled = False
            return
        
        # Load configuration
        self.selected_stations = self._load_station_selection()
        self.field_mappings = self._load_field_mappings()
        
        # Validate configuration
        if not self.validate_essential_config():
            log.error("Configuration validation failed - service disabled")
            self.service_enabled = False
            return
            
        if not self.selected_stations:
            log.error("No stations selected - service disabled")
            self.service_enabled = False
            return
            
        if not self.field_mappings:
            log.error("No field mappings found - service disabled")
            self.service_enabled = False
            return
        
        # Initialize API clients
        timeout = int(self.service_config.get('timeout', 30))
        retry_attempts = int(self.service_config.get('retry_attempts', 3))
        
        self.coops_client = COOPSAPIClient(timeout=timeout, retry_attempts=retry_attempts)
        self.ndbc_client = NDBCAPIClient(timeout=timeout)
        
        # Start background data collection threads
        self._start_background_threads()
        
        # Start thread health monitoring
        self._start_health_monitor()
        
        # NO binding to NEW_ARCHIVE_RECORD - we use dedicated tables
        
        log.info("Marine Data service initialized successfully")
        self.service_enabled = True

    def _get_today_tides(self, db_manager):
        """Get all tides for today"""
        try:
            # Get start and end of today
            now = datetime.now()
            today_start = int(datetime.combine(now.date(), datetime.min.time()).timestamp())
            today_end = today_start + 86400  # 24 hours later
            
            sql = """
                SELECT tide_time, tide_type, predicted_height, station_id
                FROM tide_table 
                WHERE tide_time >= ? AND tide_time < ?
                ORDER BY tide_time
            """
            
            result = db_manager.connection.execute(sql, (today_start, today_end))
            tides = []
            
            for row in result.fetchall():
                tides.append({
                    'time': row[0],
                    'type': 'High' if row[1] == 'H' else 'Low',
                    'height': row[2],
                    'station_id': row[3],
                    'formatted_time': datetime.fromtimestamp(row[0]).strftime('%I:%M %p'),
                    'formatted_height': f"{row[2]:.1f} ft"
                })
            
            return tides
            
        except Exception as e:
            log.error(f"Error getting today's tides: {e}")
        return []

    def _get_week_tides(self, db_manager):
        """Get 7-day tide forecast organized by day"""
        try:
            current_time = int(time.time())
            week_end = current_time + (7 * 86400)  # 7 days from now
            
            sql = """
                SELECT tide_time, tide_type, predicted_height, station_id, days_ahead
                FROM tide_table 
                WHERE tide_time >= ? AND tide_time < ?
                ORDER BY tide_time
            """
            
            result = db_manager.connection.execute(sql, (current_time, week_end))
            week_tides = {}
            
            for row in result.fetchall():
                tide_time = row[0]
                tide_date = datetime.fromtimestamp(tide_time).date()
                date_key = tide_date.strftime('%Y-%m-%d')
                
                if date_key not in week_tides:
                    week_tides[date_key] = {
                        'date': tide_date.strftime('%A, %B %d'),
                        'tides': []
                    }
                
                week_tides[date_key]['tides'].append({
                    'time': tide_time,
                    'type': 'High' if row[1] == 'H' else 'Low',
                    'height': row[2],
                    'station_id': row[3],
                    'days_ahead': row[4],
                    'formatted_time': datetime.fromtimestamp(tide_time).strftime('%I:%M %p'),
                    'formatted_height': f"{row[2]:.1f} ft"
                })
            
            return week_tides
            
        except Exception as e:
            log.error(f"Error getting week tides: {e}")
        return {}

    def _load_station_selection(self):
        """SUCCESS MANUAL PATTERN: Load station selection from config_dict"""
        # Step 1: Get service section
        service_config = self.config_dict.get('MarineDataService', {})
        
        # Step 2: Get selected_stations subsection
        selected_stations_config = service_config.get('selected_stations', {})
        
        stations = {}
        
        # Step 3: Get module data - coops_stations
        coops_stations = selected_stations_config.get('coops_stations', {})
        if coops_stations:
            enabled_coops = [station_id for station_id, enabled in coops_stations.items() if enabled.lower() == 'true']
            if enabled_coops:
                stations['coops_module'] = enabled_coops
        
        # Step 3: Get module data - ndbc_stations
        ndbc_stations = selected_stations_config.get('ndbc_stations', {})
        if ndbc_stations:
            enabled_ndbc = [station_id for station_id, enabled in ndbc_stations.items() if enabled.lower() == 'true']
            if enabled_ndbc:
                stations['ndbc_module'] = enabled_ndbc
        
        log.info(f"Loaded station selection: {stations}")
        return stations

    def _load_field_mappings(self):
        """SUCCESS MANUAL PATTERN: Load field mappings from config_dict"""
        # Step 1: Get service section
        service_config = self.config_dict.get('MarineDataService', {})
        
        # Step 2: Get field_mappings subsection
        field_mappings = service_config.get('field_mappings', {})
        
        log.info(f"Loaded field mappings for {len(field_mappings)} modules")
        return field_mappings

    def validate_essential_config(self):
        """ITEM 2: Validate that all essential configuration sections are present"""
        required_sections = [
            'selected_stations',   # Station configuration
            'field_mappings',      # Data extraction mappings
        ]
        
        errors = []
        
        for section in required_sections:
            if section not in self.service_config:
                errors.append(f"Missing required configuration section: {section}")
        
        # Validate selected_stations structure
        selected_stations = self.service_config.get('selected_stations', {})
        if not any(selected_stations.values()):
            errors.append("No stations enabled in selected_stations configuration")
        
        # Validate field_mappings structure
        field_mappings = self.service_config.get('field_mappings', {})
        if not field_mappings:
            errors.append("No field mappings found in configuration")
        else:
            for module_name, module_fields in field_mappings.items():
                if not isinstance(module_fields, dict):
                    errors.append(f"Invalid field mappings structure for module: {module_name}")
                    continue
                
                for field_name, field_config in module_fields.items():
                    if not isinstance(field_config, dict):
                        errors.append(f"Invalid field config for {module_name}.{field_name}")
                        continue
                    
                    required_keys = ['database_field', 'database_table']
                    for key in required_keys:
                        if key not in field_config:
                            errors.append(f"Missing {key} in {module_name}.{field_name}")
        
        if errors:
            for error in errors:
                log.error(f"Configuration validation error: {error}")
            return False
        
        log.info("Configuration validation passed")
        return True

    def _start_background_threads(self):
        """Start background data collection threads with WeeWX manager"""
        
        # Start CO-OPS thread if stations configured
        coops_stations = self.selected_stations.get('coops_module', [])
        if coops_stations:
            coops_fields = self.field_mappings.get('coops_module', {})
            self.coops_thread = COOPSBackgroundThread(
                coops_stations, 
                coops_fields, 
                self.coops_client,
                self.db_manager,
                self.service_config
            )
            self.coops_thread.daemon = True
            self.coops_thread.start()
            log.info(f"CO-OPS background thread started for stations: {coops_stations}")
        
        # Start NDBC thread if stations configured
        ndbc_stations = self.selected_stations.get('ndbc_module', [])
        if ndbc_stations:
            ndbc_fields = self.field_mappings.get('ndbc_module', {})
            self.ndbc_thread = NDBCBackgroundThread(
                ndbc_stations,
                ndbc_fields,
                self.ndbc_client,
                self.db_manager,
                self.service_config
            )
            self.ndbc_thread.daemon = True
            self.ndbc_thread.start()
            log.info(f"NDBC background thread started for stations: {ndbc_stations}")

    def _start_health_monitor(self):
        """ITEM 10: Start background thread health monitoring"""
        self.health_monitor = ThreadHealthMonitor(
            service=self,
            check_interval=300  # Check every 5 minutes
        )
        self.health_monitor.daemon = True
        self.health_monitor.start()
        log.info("Thread health monitor started")
        

class ThreadHealthMonitor(threading.Thread):
    """ITEM 10: Background thread health monitoring and restart capability"""
    
    def __init__(self, service, check_interval=300):
        super().__init__(daemon=True, name='ThreadHealthMonitor')
        self.service = service
        self.check_interval = check_interval
        self.running = True
        self.last_check = {}

    def run(self):
        """Monitor background thread health and restart if needed"""
        log.info("Thread health monitor started")
        
        while self.running:
            try:
                time.sleep(self.check_interval)
                
                if not self.running:
                    break
                
                self._check_coops_thread()
                self._check_ndbc_thread()
                
            except Exception as e:
                log.error(f"Health monitor error: {e}")
                time.sleep(60)  # Wait before retrying

    def _check_coops_thread(self):
        """Check CO-OPS thread health and restart if needed"""
        if not hasattr(self.service, 'coops_thread') or not self.service.coops_thread:
            return
        
        thread = self.service.coops_thread
        
        # Check if thread is alive
        if not thread.is_alive():
            log.warning("CO-OPS thread died - attempting restart")
            self._restart_coops_thread()
            return
        
        # Check if thread is making progress (collecting data)
        current_time = time.time()
        thread_name = 'coops'
        
        if hasattr(thread, 'last_successful_collection'):
            last_collection = thread.last_successful_collection
            time_since_collection = current_time - last_collection
            
            # If no collection in 2 hours (should collect every 10 minutes)
            if time_since_collection > 7200:
                log.warning(f"CO-OPS thread appears stuck - no collection in {time_since_collection/60:.1f} minutes")
                self._restart_coops_thread()

    def _check_ndbc_thread(self):
        """Check NDBC thread health and restart if needed"""
        if not hasattr(self.service, 'ndbc_thread') or not self.service.ndbc_thread:
            return
        
        thread = self.service.ndbc_thread
        
        # Check if thread is alive
        if not thread.is_alive():
            log.warning("NDBC thread died - attempting restart")
            self._restart_ndbc_thread()
            return
        
        # Check if thread is making progress
        current_time = time.time()
        
        if hasattr(thread, 'last_successful_collection'):
            last_collection = thread.last_successful_collection
            time_since_collection = current_time - last_collection
            
            # If no collection in 3 hours (should collect every hour)
            if time_since_collection > 10800:
                log.warning(f"NDBC thread appears stuck - no collection in {time_since_collection/60:.1f} minutes")
                self._restart_ndbc_thread()

    def _restart_coops_thread(self):
        """Restart CO-OPS background thread"""
        try:
            # Stop old thread
            if hasattr(self.service, 'coops_thread') and self.service.coops_thread:
                self.service.coops_thread.running = False
                self.service.coops_thread.join(timeout=5)
            
            # Start new thread
            coops_stations = self.service.selected_stations.get('coops_module', [])
            if coops_stations:
                coops_fields = self.service.field_mappings.get('coops_module', {})
                self.service.coops_thread = COOPSBackgroundThread(
                    coops_stations, 
                    coops_fields, 
                    self.service.coops_client,
                    self.service.db_manager,
                    self.service.service_config
                )
                self.service.coops_thread.daemon = True
                self.service.coops_thread.start()
                log.info("CO-OPS thread restarted successfully")
            
        except Exception as e:
            log.error(f"Failed to restart CO-OPS thread: {e}")

    def _restart_ndbc_thread(self):
        """Restart NDBC background thread"""
        try:
            # Stop old thread
            if hasattr(self.service, 'ndbc_thread') and self.service.ndbc_thread:
                self.service.ndbc_thread.running = False
                self.service.ndbc_thread.join(timeout=5)
            
            # Start new thread
            ndbc_stations = self.service.selected_stations.get('ndbc_module', [])
            if ndbc_stations:
                ndbc_fields = self.service.field_mappings.get('ndbc_module', {})
                self.service.ndbc_thread = NDBCBackgroundThread(
                    ndbc_stations,
                    ndbc_fields,
                    self.service.ndbc_client,
                    self.service.db_manager,
                    self.service.service_config
                )
                self.service.ndbc_thread.daemon = True
                self.service.ndbc_thread.start()
                log.info("NDBC thread restarted successfully")
            
        except Exception as e:
            log.error(f"Failed to restart NDBC thread: {e}")


# FUNCTIONAL: Testing and debugging interface
class MarineDataTester:
    
    def __init__(self):
        self.config_dict = None
        self.service_config = None
        self._load_weewx_config()

    def _load_weewx_config(self):
        """
        FIXED: Load WeeWX configuration using standard discovery patterns
        
        CORRECTIONS:
        - Uses WeeWX 5.1 standard configuration paths
        - Follows success manual configuration loading patterns
        - Proper error handling with graceful degradation
        """
        # WeeWX 5.1 standard configuration paths (in order of preference)
        config_paths = [
            '/etc/weewx/weewx.conf',              # Debian/Ubuntu package install
            '/home/weewx/weewx-data/weewx.conf',  # pip install method
            '/opt/weewx/weewx.conf',              # Custom install location
            os.path.expanduser('~/weewx-data/weewx.conf'),  # User home install
            './weewx.conf'                        # Current directory (development)
        ]
        
        for config_path in config_paths:
            if os.path.exists(config_path):
                try:
                    import configobj
                    self.config_dict = configobj.ConfigObj(config_path)
                    
                    # Load service-specific configuration following success manual pattern
                    self.service_config = self.config_dict.get('MarineDataService', {})
                    
                    print(f"{CORE_ICONS['status']} Loaded WeeWX configuration: {config_path}")
                    return
                    
                except Exception as e:
                    print(f"{CORE_ICONS['warning']} Failed to load {config_path}: {e}")
                    continue
        
        print(f"{CORE_ICONS['warning']} No WeeWX configuration found in standard locations")

    def test_installation(self):
        """
        FIXED: Test basic installation components
        
        CORRECTIONS:
        - Fixed corrupted service registration check
        - Uses proper error handling patterns
        - Clear success/failure reporting
        """
        print(f"\n{CORE_ICONS['selection']} TESTING INSTALLATION")
        print("-" * 40)
        
        if not self.config_dict:
            print(f"{CORE_ICONS['warning']} No WeeWX configuration available")
            return False
        
        success = True
        
        # FIXED: Check service registration (was corrupted before)
        print("Checking service registration...")
        try:
            engine_config = self.config_dict.get('Engine', {})
            services_config = engine_config.get('Services', {})
            data_services = services_config.get('data_services', '')
            
            # FIXED: Check for correct service name (was corrupted with file path)
            if 'user.marine_data.MarineDataService' in data_services:
                print(f"  {CORE_ICONS['status']} MarineDataService registered in data_services")
            else:
                print(f"  {CORE_ICONS['warning']} MarineDataService NOT found in data_services")
                print(f"      Current data_services: {data_services}")
                success = False
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} Error checking service registration: {e}")
            success = False
        
        # Check service configuration section
        print("Checking service configuration...")
        try:
            if 'MarineDataService' in self.config_dict:
                service_config = self.config_dict['MarineDataService']
                print(f"  {CORE_ICONS['status']} MarineDataService configuration found")
                
                # Check required configuration items
                required_items = ['enable', 'coops_stations', 'ndbc_stations']
                for item in required_items:
                    if item in service_config:
                        print(f"  {CORE_ICONS['status']} {item}: {service_config[item]}")
                    else:
                        print(f"  {CORE_ICONS['warning']} Missing required config: {item}")
                        success = False
                        
            else:
                print(f"  {CORE_ICONS['warning']} MarineDataService configuration section not found")
                success = False
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} Error checking service configuration: {e}")
            success = False
        
        # Check database tables
        print("Checking database tables...")
        try:
            tables = self._get_database_tables()
            required_tables = ['coops_realtime', 'tide_table', 'ndbc_data']
            
            for table in required_tables:
                if table in tables:
                    print(f"  {CORE_ICONS['status']} {table} table exists")
                else:
                    print(f"  {CORE_ICONS['warning']} {table} table missing")
                    success = False
                    
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} Error checking database tables: {e}")
            success = False
        
        return success

    def test_api_connectivity(self):
        """
        FIXED: Test API connectivity
        
        CORRECTIONS:
        - Follows success manual error handling patterns
        - Uses proper timeout and retry logic
        - Clear success/failure reporting
        """
        print(f"\n{CORE_ICONS['selection']} TESTING API CONNECTIVITY")
        print("-" * 40)
        
        success = True
        
        # Test CO-OPS API
        print("Testing CO-OPS API connectivity...")
        try:
            # Use a known good station for testing
            test_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=water_level&station=8454000&units=english&time_zone=lst_ldt&format=json&date=latest"
            
            import urllib.request
            import urllib.error
            
            # Follow success manual timeout patterns
            request = urllib.request.Request(test_url)
            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                
            if 'data' in data and len(data['data']) > 0:
                print(f"  {CORE_ICONS['status']} CO-OPS API responding correctly")
            else:
                print(f"  {CORE_ICONS['warning']} CO-OPS API returned no data")
                success = False
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} CO-OPS API connectivity failed: {e}")
            success = False
        
        # Test NDBC API
        print("Testing NDBC API connectivity...")
        try:
            # Use a known good buoy for testing
            test_url = "https://www.ndbc.noaa.gov/data/realtime2/44013.txt"
            
            request = urllib.request.Request(test_url)
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read().decode('utf-8')
                
            if len(data) > 100 and 'YY' in data:  # Basic validation
                print(f"  {CORE_ICONS['status']} NDBC API responding correctly")
            else:
                print(f"  {CORE_ICONS['warning']} NDBC API returned unexpected data")
                success = False
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} NDBC API connectivity failed: {e}")
            success = False
        
        return success

    def test_database_operations(self):
        """
        FIXED: Test database operations
        
        CORRECTIONS:
        - Uses WeeWX 5.1 database manager patterns
        - Follows success manual database access
        - Proper transaction handling
        """
        print(f"\n{CORE_ICONS['selection']} TESTING DATABASE OPERATIONS")
        print("-" * 40)
        
        if not self.config_dict:
            print(f"{CORE_ICONS['warning']} No WeeWX configuration available")
            return False
        
        success = True
        
        try:
            # FIXED: Use WeeWX 5.1 database manager pattern
            with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as db_manager:
                
                # Test table existence and structure
                print("Testing table structure...")
                
                required_tables = {
                    'coops_realtime': ['dateTime', 'station_id', 'water_level', 'water_temp'],
                    'tide_table': ['tide_time', 'station_id', 'predicted_height', 'tide_type'],
                    'ndbc_data': ['dateTime', 'station_id', 'wave_height', 'wave_period', 'wind_speed']
                }
                
                for table_name, expected_columns in required_tables.items():
                    try:
                        # Test basic table access
                        sql = f"SELECT COUNT(*) FROM {table_name}"
                        result = db_manager.connection.execute(sql)
                        count = result.fetchone()[0]
                        
                        print(f"  {CORE_ICONS['status']} {table_name}: {count} records")
                        
                        # Test column existence (MySQL vs SQLite compatible)
                        try:
                            # Try MySQL DESCRIBE first
                            result = db_manager.connection.execute(f"DESCRIBE {table_name}")
                            columns = [row[0] for row in result.fetchall()]
                        except:
                            # Fall back to SQLite PRAGMA
                            result = db_manager.connection.execute(f"PRAGMA table_info({table_name})")
                            columns = [row[1] for row in result.fetchall()]
                        
                        missing_columns = set(expected_columns) - set(columns)
                        if missing_columns:
                            print(f"    {CORE_ICONS['warning']} Missing columns: {missing_columns}")
                            success = False
                        else:
                            print(f"    {CORE_ICONS['status']} All required columns present")
                            
                    except Exception as e:
                        print(f"  {CORE_ICONS['warning']} Error accessing {table_name}: {e}")
                        success = False
                
                # Test basic insert/query operation
                print("Testing database write/read operations...")
                try:
                    # Test with coops_realtime table
                    test_time = int(time.time())
                    test_sql = """
                        INSERT OR REPLACE INTO coops_realtime 
                        (dateTime, station_id, water_level, interval, usUnits) 
                        VALUES (?, ?, ?, ?, ?)
                    """
                    
                    db_manager.connection.execute(test_sql, (test_time, 'TEST', 5.5, 'archive', 1))
                    db_manager.connection.commit()
                    
                    # Verify insert
                    verify_sql = "SELECT water_level FROM coops_realtime WHERE station_id = 'TEST' AND dateTime = ?"
                    result = db_manager.connection.execute(verify_sql, (test_time,))
                    row = result.fetchone()
                    
                    if row and abs(row[0] - 5.5) < 0.01:
                        print(f"  {CORE_ICONS['status']} Database write/read operations working")
                        
                        # Clean up test data
                        cleanup_sql = "DELETE FROM coops_realtime WHERE station_id = 'TEST'"
                        db_manager.connection.execute(cleanup_sql)
                        db_manager.connection.commit()
                    else:
                        print(f"  {CORE_ICONS['warning']} Database write/read verification failed")
                        success = False
                        
                except Exception as e:
                    print(f"  {CORE_ICONS['warning']} Database operation test failed: {e}")
                    success = False
                    
        except Exception as e:
            print(f"{CORE_ICONS['warning']} Database connection failed: {e}")
            success = False
        
        return success

    def run_all_tests(self):
        """
        FIXED: Run comprehensive test suite
        
        CORRECTIONS:
        - Clear progress reporting
        - Proper success/failure summary
        - Uses 4-icon standard
        """
        print(f"\n{CORE_ICONS['navigation']} MARINE DATA EXTENSION - COMPREHENSIVE TESTING")
        print("=" * 60)
        
        tests = [
            ("Installation", self.test_installation),
            ("API Connectivity", self.test_api_connectivity), 
            ("Database Operations", self.test_database_operations)
        ]
        
        results = []
        
        for test_name, test_func in tests:
            print(f"\nRunning {test_name} tests...")
            try:
                success = test_func()
                results.append((test_name, success))
                
                if success:
                    print(f"{CORE_ICONS['status']} {test_name} tests PASSED")
                else:
                    print(f"{CORE_ICONS['warning']} {test_name} tests FAILED")
                    
            except Exception as e:
                print(f"{CORE_ICONS['warning']} {test_name} tests ERROR: {e}")
                results.append((test_name, False))
        
        # Summary
        print(f"\n{CORE_ICONS['selection']} TEST SUMMARY")
        print("-" * 30)
        
        passed_tests = sum(1 for _, success in results if success)
        total_tests = len(results)
        
        for test_name, success in results:
            status_icon = CORE_ICONS['status'] if success else CORE_ICONS['warning']
            status_text = "PASS" if success else "FAIL"
            print(f"  {status_icon} {test_name}: {status_text}")
        
        print(f"\nOverall: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            print(f"{CORE_ICONS['status']} ALL TESTS PASSED")
        else:
            print(f"{CORE_ICONS['warning']} SOME TESTS FAILED")
        
        return passed_tests == total_tests

    def _get_database_tables(self):
        """
        FIXED: Get list of database tables using WeeWX 5.1 patterns
        
        CORRECTIONS:
        - Uses proper database manager access
        - Compatible with both MySQL and SQLite
        - Follows success manual patterns
        """
        with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as manager:
            try:
                # Try MySQL first
                result = manager.connection.execute("SHOW TABLES")
                return [row[0] for row in result.fetchall()]
            except:
                # Fall back to SQLite
                result = manager.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
                return [row[0] for row in result.fetchall()]


class COOPSBackgroundThread(threading.Thread):
    """
    FUNCTIONAL: CO-OPS background thread with real data collection
    """
    
    def __init__(self, stations, fields, api_client, db_manager, config):
        super().__init__(daemon=True, name='COOPSBackgroundThread')
        self.stations = stations
        self.fields = fields
        self.api_client = api_client
        self.db_manager = db_manager
        self.config = config
        self.running = True
        self.last_successful_collection = time.time()  # ITEM 10: Track for health monitoring
        
        # Collection intervals
        self.water_level_interval = int(config.get('coops_collection_interval', 600))  # 10 minutes
        self.predictions_interval = int(config.get('tide_predictions_interval', 21600))  # 6 hours

    def run(self):
        """FUNCTIONAL: Real data collection loop"""
        log.info("CO-OPS background thread started")
        
        last_water_level = 0
        last_predictions = 0
        
        while self.running:
            try:
                current_time = time.time()
                
                # Collect water level data
                if current_time - last_water_level >= self.water_level_interval:
                    self._collect_water_level_data()
                    last_water_level = current_time
                    self.last_successful_collection = current_time  # ITEM 10: Update for health monitoring
                
                # Collect tide predictions
                if current_time - last_predictions >= self.predictions_interval:
                    self._collect_tide_predictions()
                    last_predictions = current_time
                    self.last_successful_collection = current_time  # ITEM 10: Update for health monitoring
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                log.error(f"CO-OPS background thread error: {e}")
                time.sleep(300)  # Wait 5 minutes on error

    def _collect_water_level_data(self):
        """FUNCTIONAL: Collect real-time water level data with graceful handling of missing data"""
        for station_id in self.stations:
            try:
                # Get current water level - may return None for stations without this capability
                data = self.api_client.get_water_level(station_id)
                if data:  # Only insert if we got actual data
                    self._insert_coops_data(station_id, data)
                else:
                    log.debug(f"Station {station_id} does not provide water level data")
                    
                # Get water temperature if available - may return None
                temp_data = self.api_client.get_water_temperature(station_id)
                if temp_data:  # Only insert if we got actual data
                    self._insert_coops_data(station_id, temp_data)
                else:
                    log.debug(f"Station {station_id} does not provide water temperature data")
                    
            except Exception as e:
                log.error(f"Error collecting water level for station {station_id}: {e}")

    def _collect_tide_predictions(self):
        """FUNCTIONAL: Collect 7-day tide predictions with graceful handling of missing data"""
        for station_id in self.stations:
            try:
                # Get 7 days of predictions - may return None for stations without this capability
                end_date = datetime.now() + timedelta(days=7)
                data = self.api_client.get_predictions(
                    station_id,
                    begin_date=datetime.now().strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d')
                )
                
                if data and 'predictions' in data:
                    self._insert_tide_predictions(station_id, data)
                else:
                    log.debug(f"Station {station_id} does not provide tide prediction data")
                    
            except Exception as e:
                log.error(f"Error collecting predictions for station {station_id}: {e}")

    def _insert_coops_data(self, station_id, data):
        """FUNCTIONAL: Insert CO-OPS data using WeeWX manager with database-aware SQL"""
        current_time = int(time.time())
        
        # Build insert data
        insert_data = {
            'dateTime': current_time,
            'station_id': station_id
        }
        
        # Map API response to database fields based on field mappings
        if 'water_level' in data:
            wl_data = data['water_level']
            insert_data['marine_current_water_level'] = wl_data.get('value')
            insert_data['marine_water_level_sigma'] = wl_data.get('sigma')
            insert_data['marine_water_level_flags'] = wl_data.get('flags')
        
        if 'water_temperature' in data:
            temp_data = data['water_temperature']
            insert_data['marine_coastal_water_temp'] = temp_data.get('value')
            insert_data['marine_water_temp_flags'] = temp_data.get('flags')
        
        # Build and execute database-aware SQL
        fields = list(insert_data.keys())
        values = list(insert_data.values())
        
        # Use database-aware upsert SQL
        sql = self._get_upsert_sql('coops_realtime', fields)
        
        # Execute using WeeWX manager
        self.db_manager.connection.execute(sql, values)
        log.debug(f"Inserted CO-OPS data for station {station_id}")

    def _insert_tide_predictions(self, station_id, data):
        """Insert tide predictions using WeeWX manager with database-aware SQL"""
        current_time = int(time.time())
        
        # Clean up old predictions - RETAIN ALL EXISTING
        cleanup_sql = "DELETE FROM tide_table WHERE station_id = ? AND tide_time < ?"
        yesterday = current_time - 86400
        self.db_manager.connection.execute(cleanup_sql, (station_id, yesterday))
        
        # Insert new predictions - RETAIN ALL EXISTING 
        for prediction in data['predictions']:
            try:
                tide_time_str = prediction.get('t')
                tide_time = int(datetime.fromisoformat(tide_time_str.replace('Z', '+00:00')).timestamp())
                tide_type = prediction.get('type', 'H')
                height = float(prediction.get('v', 0))
                
                prediction_date = datetime.fromtimestamp(tide_time)
                current_date = datetime.fromtimestamp(current_time)
                days_ahead = (prediction_date.date() - current_date.date()).days
                
                fields = ['dateTime', 'station_id', 'tide_time', 'tide_type', 'predicted_height', 'datum', 'days_ahead']
                sql = self._get_upsert_sql('tide_table', fields)
                
                self.db_manager.connection.execute(sql, (
                    current_time, station_id, tide_time, tide_type, height, 'MLLW', days_ahead
                ))
                
            except Exception as e:
                log.error(f"Error inserting tide prediction: {e}")
        
        log.debug(f"Updated tide predictions for station {station_id}")
        
        # SURGICAL FIX: ADD ONLY THIS LINE
        self._update_tide_summaries(station_id, current_time)

    def _update_tide_summaries(self, station_id, current_time):
        """Calculate and update next high/low tide summary fields using WeeWX manager connection"""
        try:
            # Use the same pattern as the working TideTableSearchList
            sql = """
                SELECT tide_type, tide_time, predicted_height
                FROM tide_table 
                WHERE station_id = ? AND tide_time > ?
                ORDER BY tide_time ASC
            """
            
            # Execute query and get cursor result
            result = self.db_manager.connection.execute(sql, (station_id, current_time))
            
            # Check if result is None
            if result is None:
                log.warning(f"Query returned None for station {station_id}")
                return
                
            # Get all rows using fetchall() - same as working TideTableSearchList
            rows = result.fetchall()
            
            # Calculate summary values in one pass
            next_high_time = next_high_height = next_low_time = next_low_height = None
            today_highs = []
            today_lows = []
            today_end = current_time + 86400
            
            for tide_type, tide_time, height in rows:
                if tide_type == 'H' and next_high_time is None:
                    next_high_time, next_high_height = tide_time, height
                elif tide_type == 'L' and next_low_time is None:
                    next_low_time, next_low_height = tide_time, height
                    
                if tide_time < today_end:
                    if tide_type == 'H':
                        today_highs.append(height)
                    else:
                        today_lows.append(height)
            
            # Calculate tide range
            tide_range = None
            if today_highs and today_lows:
                tide_range = max(today_highs) - min(today_lows)
            
            # Update summary fields - use same pattern as other inserts
            update_sql = """
                UPDATE tide_table 
                SET marine_next_high_time = ?, 
                    marine_next_high_height = ?,
                    marine_next_low_time = ?,
                    marine_next_low_height = ?,
                    marine_tide_range = ?
                WHERE station_id = ? AND dateTime = (SELECT MAX(dateTime) FROM tide_table WHERE station_id = ?)
            """
            
            # Execute update
            self.db_manager.connection.execute(update_sql, (
                next_high_time, next_high_height, next_low_time, next_low_height, 
                tide_range, station_id, current_time
            ))
            
            log.debug(f"Updated tide summaries for station {station_id}: next_high={next_high_time}, next_low={next_low_time}, range={tide_range}")
            
        except Exception as e:
            log.error(f"Error updating tide summaries for station {station_id}: {e}")
            import traceback
            log.error(traceback.format_exc())

    def _get_database_type(self):
        """Detect database type through WeeWX manager connection"""
        try:
            # Test for MySQL/MariaDB by trying MySQL-specific function
            cursor = self.db_manager.connection.cursor()
            cursor.execute("SELECT VERSION()")
            cursor.fetchone()
            cursor.close()
            return 'mysql'
        except Exception:
            # If MySQL command fails, assume SQLite
            return 'sqlite'

    def _get_upsert_sql(self, table_name, fields):
        """Get database-appropriate upsert SQL through WeeWX manager"""
        field_list = ', '.join(fields)
        placeholders = ', '.join(['?' if self._get_database_type() == 'sqlite' else '%s'] * len(fields))
        
        db_type = self._get_database_type()
        
        if db_type == 'mysql':
            # MySQL/MariaDB syntax
            return f"REPLACE INTO {table_name} ({field_list}) VALUES ({placeholders})"
        else:
            # SQLite syntax
            return f"INSERT OR REPLACE INTO {table_name} ({field_list}) VALUES ({placeholders})"
    

class NDBCBackgroundThread(threading.Thread):
    """
    FUNCTIONAL: NDBC background thread with real data collection
    """
    
    def __init__(self, stations, fields, api_client, db_manager, config):
        super().__init__(daemon=True, name='NDBCBackgroundThread')
        self.stations = stations
        self.fields = fields
        self.api_client = api_client
        self.db_manager = db_manager
        self.config = config
        self.running = True
        self.last_successful_collection = time.time()  # ITEM 10: Track for health monitoring
        
        # Collection interval
        self.collection_interval = int(config.get('ndbc_weather_interval', 3600))  # 1 hour

    def run(self):
        """FUNCTIONAL: Real NDBC data collection loop"""
        log.info("NDBC background thread started")
        
        last_collection = 0
        
        while self.running:
            try:
                current_time = time.time()
                
                # Collect NDBC data
                if current_time - last_collection >= self.collection_interval:
                    self._collect_ndbc_data()
                    last_collection = current_time
                    self.last_successful_collection = current_time  # ITEM 10: Update for health monitoring
                
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                log.error(f"NDBC background thread error: {e}")
                time.sleep(300)

    def _collect_ndbc_data(self):
        """FUNCTIONAL: Collect NDBC buoy data"""
        for station_id in self.stations:
            try:
                data = self.api_client.get_station_data(station_id)
                if data:
                    self._insert_ndbc_data(station_id, data)
                    
            except Exception as e:
                log.error(f"Error collecting NDBC data for station {station_id}: {e}")

    def _insert_ndbc_data(self, station_id, data):
        """FUNCTIONAL: Insert NDBC data using WeeWX manager with database-aware SQL"""
        current_time = int(time.time())
        
        # Build insert data with unit conversions
        insert_data = {
            'dateTime': current_time,
            'station_id': station_id
        }
        
        # Field mapping with unit conversions
        field_mapping = {
            'WVHT': ('marine_wave_height', lambda x: float(x) * 3.28084),  # m to ft
            'DPD': ('marine_wave_period', float),
            'MWD': ('marine_wave_direction', float),
            'WSPD': ('marine_wind_speed', lambda x: float(x) * 2.23694),  # m/s to mph
            'WDIR': ('marine_wind_direction', float),
            'GST': ('marine_wind_gust', lambda x: float(x) * 2.23694),  # m/s to mph
            'ATMP': ('marine_air_temp', lambda x: float(x) * 9/5 + 32),  # C to F
            'WTMP': ('marine_sea_surface_temp', lambda x: float(x) * 9/5 + 32),  # C to F
            'PRES': ('marine_barometric_pressure', lambda x: float(x) * 0.0295301),  # hPa to inHg
            'VIS': ('marine_visibility', float),
            'DEWP': ('marine_dewpoint', lambda x: float(x) * 9/5 + 32)  # C to F
        }
        
        # Convert NDBC data to database fields
        for ndbc_field, (db_field, converter) in field_mapping.items():
            if ndbc_field in data and data[ndbc_field] is not None and data[ndbc_field] != 'MM':
                try:
                    value = converter(data[ndbc_field])
                    insert_data[db_field] = value
                except (ValueError, TypeError):
                    continue
        
        # Build and execute database-aware SQL
        if len(insert_data) > 2:  # More than just dateTime and station_id
            fields = list(insert_data.keys())
            values = list(insert_data.values())
            
            # Use database-aware upsert SQL
            sql = self._get_upsert_sql('ndbc_data', fields)
            
            # Execute using WeeWX manager
            self.db_manager.connection.execute(sql, values)
            log.debug(f"Inserted NDBC data for station {station_id}")

    def _get_database_type(self):
        """Detect database type through WeeWX manager connection"""
        try:
            # Test for MySQL/MariaDB by trying MySQL-specific function
            cursor = self.db_manager.connection.cursor()
            cursor.execute("SELECT VERSION()")
            cursor.fetchone()
            cursor.close()
            return 'mysql'
        except Exception:
            # If MySQL command fails, assume SQLite
            return 'sqlite'

    def _get_upsert_sql(self, table_name, fields):
        """Get database-appropriate upsert SQL through WeeWX manager"""
        field_list = ', '.join(fields)
        placeholders = ', '.join(['?' if self._get_database_type() == 'sqlite' else '%s'] * len(fields))
        
        db_type = self._get_database_type()
        
        if db_type == 'mysql':
            # MySQL/MariaDB syntax
            return f"REPLACE INTO {table_name} ({field_list}) VALUES ({placeholders})"
        else:
            # SQLite syntax
            return f"INSERT OR REPLACE INTO {table_name} ({field_list}) VALUES ({placeholders})"


class COOPSAPIClient:
    """
    FUNCTIONAL: CO-OPS API client with real HTTP requests
    """
    
    def __init__(self, timeout=30, retry_attempts=3):
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

    def get_water_level(self, station_id):
        """FUNCTIONAL: Get current water level data"""
        params = {
            'product': 'water_level',
            'application': 'WeeWX_Marine_Extension',
            'station': station_id,
            'date': 'latest',
            'datum': 'MLLW',
            'units': 'english',
            'time_zone': 'gmt',
            'format': 'json'
        }
        
        return self._make_api_request(params)

    def get_water_temperature(self, station_id):
        """FUNCTIONAL: Get water temperature data"""
        params = {
            'product': 'water_temperature',
            'application': 'WeeWX_Marine_Extension',
            'station': station_id,
            'date': 'latest',
            'units': 'english',
            'time_zone': 'gmt',
            'format': 'json'
        }
        
        return self._make_api_request(params)

    def get_predictions(self, station_id, begin_date, end_date):
        """FUNCTIONAL: Get tide predictions"""
        params = {
            'product': 'predictions',
            'application': 'WeeWX_Marine_Extension',
            'station': station_id,
            'begin_date': begin_date,
            'end_date': end_date,
            'datum': 'MLLW',
            'units': 'english',
            'time_zone': 'gmt',
            'format': 'json',
            'interval': 'hilo'
        }
        
        return self._make_api_request(params)

    def _make_api_request(self, params):
        """FUNCTIONAL: Make HTTP request with retries and handle missing data gracefully"""
        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
        
        for attempt in range(self.retry_attempts):
            try:
                with urllib.request.urlopen(url, timeout=self.timeout) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    
                    # Check for API errors
                    if 'error' in data:
                        error_msg = data['error'].get('message', str(data['error']))
                        
                        # Handle "No data found" gracefully - this is normal for some stations
                        if 'No data was found' in error_msg or 'not be offered at this station' in error_msg:
                            log.debug(f"CO-OPS station does not provide this data type: {error_msg}")
                            return None  # Return None instead of raising exception
                        
                        # Other errors are still actual problems
                        raise MarineDataAPIError(f"CO-OPS API error: {data['error']}")
                    
                    return data
                    
            except (urllib.error.URLError, socket.timeout, json.JSONDecodeError) as e:
                log.warning(f"CO-OPS API attempt {attempt + 1} failed: {e}")
                if attempt == self.retry_attempts - 1:
                    raise MarineDataAPIError(f"CO-OPS API failed after {self.retry_attempts} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff


class NDBCAPIClient:
    """
    FUNCTIONAL: NDBC API client with real HTTP requests
    """
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.base_url = "https://www.ndbc.noaa.gov/data/realtime2"

    def get_station_data(self, station_id):
        """FUNCTIONAL: Get NDBC station data"""
        url = f"{self.base_url}/{station_id}.txt"
        
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as response:
                content = response.read().decode('utf-8')
                return self._parse_ndbc_data(content)
                
        except (urllib.error.URLError, socket.timeout) as e:
            raise MarineDataAPIError(f"NDBC API error for station {station_id}: {e}")

    def _parse_ndbc_data(self, content):
        """FUNCTIONAL: Parse NDBC text data format"""
        lines = content.strip().split('\n')
        if len(lines) < 3:
            return None
        
        # Parse header and units lines
        headers = lines[0].split()
        units = lines[1].split()
        
        # Get most recent data line
        data_line = lines[2].split()
        
        if len(data_line) != len(headers):
            return None
        
        # Build data dictionary
        data = {}
        for i, header in enumerate(headers):
            if i < len(data_line):
                value = data_line[i]
                if value != 'MM':  # MM = Missing data
                    try:
                        data[header] = float(value)
                    except ValueError:
                        data[header] = value
        
        return data


class TideTableSearchList(SearchList):
    """
    FIXED: WeeWX Search List Extension for tide table access
    
    CORRECTIONS:
    - Uses db_lookup parameter per WeeWX 5.1 documentation
    - Uses timespan parameter for time-bounded queries
    - Implements missing helper methods
    - Respects 7-day maximum while allowing flexible ranges
    """
    
    def __init__(self, generator):
        """WeeWX 5.1 compliant SearchList inheritance"""
        SearchList.__init__(self, generator)
        
    def get_extension_list(self, timespan, db_lookup):
        """
        FIXED: Provide tide table data for WeeWX templates
        
        CORRECTIONS:
        - Now uses both timespan and db_lookup parameters
        - Follows WeeWX 5.1 documentation patterns
        - Calculates time range from timespan with 7-day maximum
        """
        search_list = {}
        
        try:
            # FIXED: Use db_lookup parameter per WeeWX 5.1 documentation
            db_manager = db_lookup('wx_binding')
            
            # FIXED: Calculate query time range from timespan parameter
            current_time = int(time.time())
            
            # Use timespan.stop as end time, but don't exceed current time
            end_time = min(timespan.stop, current_time)
            
            # Calculate start time from timespan, but respect 7-day maximum
            requested_duration = timespan.stop - timespan.start
            max_duration = 7 * 86400  # 7 days in seconds
            
            if requested_duration > max_duration:
                # Limit to 7 days maximum
                start_time = end_time - max_duration
                log.debug(f"Tide query limited to 7 days (requested {requested_duration/86400:.1f} days)")
            else:
                # Use requested timespan
                start_time = max(timespan.start, current_time)  # Don't go into past beyond current time
            
            # Get tide information using calculated time range
            search_list['next_high_tide'] = self._get_next_tide(db_manager, 'H', current_time, end_time)
            search_list['next_low_tide'] = self._get_next_tide(db_manager, 'L', current_time, end_time)
            search_list['today_tides'] = self._get_today_tides(db_manager, current_time)
            search_list['week_tides'] = self._get_week_tides(db_manager, start_time, end_time)
            search_list['tide_range_today'] = self._get_tide_range_today(db_manager, current_time)
            
            log.debug(f"TideTableSearchList: Generated tide data for {(end_time-start_time)/86400:.1f} day range")
            
        except Exception as e:
            log.error(f"Error in TideTableSearchList: {e}")
            # Return empty dict on error rather than failing template generation
            search_list = {
                'next_high_tide': None,
                'next_low_tide': None,
                'today_tides': [],
                'week_tides': {},
                'tide_range_today': None
            }
            
        # Return proper format for WeeWX SearchList
        return [search_list]

    def _get_next_tide(self, db_manager, tide_type, start_time, end_time):
        """
        IMPLEMENTED: Get next high or low tide within timespan
        
        Args:
            db_manager: Database manager from db_lookup
            tide_type: 'H' for high, 'L' for low
            start_time: Start of search range (Unix timestamp)
            end_time: End of search range (Unix timestamp)
        """
        try:
            sql = """
                SELECT tide_time, predicted_height, station_id, datum
                FROM tide_table 
                WHERE tide_type = ? AND tide_time >= ? AND tide_time <= ?
                ORDER BY tide_time LIMIT 1
            """
            
            result = db_manager.connection.execute(sql, (tide_type, start_time, end_time))
            row = result.fetchone()
            
            if row:
                tide_time = row[0]
                return {
                    'time': tide_time,
                    'height': row[1],
                    'station_id': row[2],
                    'datum': row[3],
                    'formatted_time': datetime.fromtimestamp(tide_time).strftime('%I:%M %p'),
                    'formatted_height': f"{row[1]:.1f} ft {row[3]}",
                    'formatted_date': datetime.fromtimestamp(tide_time).strftime('%A, %B %d'),
                    'type': 'High' if tide_type == 'H' else 'Low'
                }

        except Exception as e:
            log.error(f"Error getting next {tide_type} tide: {e}")
        return None

    def _get_today_tides(self, db_manager, current_time):
        """
        IMPLEMENTED: Get all tides for today
        
        Args:
            db_manager: Database manager from db_lookup
            current_time: Current time (Unix timestamp)
        """
        try:
            # Calculate today's date boundaries
            today_date = datetime.fromtimestamp(current_time).date()
            today_start = int(datetime.combine(today_date, datetime.min.time()).timestamp())
            today_end = today_start + 86400  # 24 hours later
            
            sql = """
                SELECT tide_time, tide_type, predicted_height, station_id, datum
                FROM tide_table 
                WHERE tide_time >= ? AND tide_time < ?
                ORDER BY tide_time
            """
            
            result = db_manager.connection.execute(sql, (today_start, today_end))
            tides = []
            
            for row in result.fetchall():
                tide_time = row[0]
                tides.append({
                    'time': tide_time,
                    'type': 'High' if row[1] == 'H' else 'Low',
                    'height': row[2],
                    'station_id': row[3],
                    'datum': row[4],
                    'formatted_time': datetime.fromtimestamp(tide_time).strftime('%I:%M %p'),
                    'formatted_height': f"{row[2]:.1f} ft {row[4]}",
                    'is_past': tide_time < current_time
                })
            
            return tides
            
        except Exception as e:
            log.error(f"Error getting today's tides: {e}")
        return []

    def _get_week_tides(self, db_manager, start_time, end_time):
        """
        IMPLEMENTED: Get tides for time range organized by day
        
        Args:
            db_manager: Database manager from db_lookup
            start_time: Start of range (Unix timestamp)
            end_time: End of range (Unix timestamp)
        """
        try:
            sql = """
                SELECT tide_time, tide_type, predicted_height, station_id, datum, days_ahead
                FROM tide_table 
                WHERE tide_time >= ? AND tide_time <= ?
                ORDER BY tide_time
            """
            
            result = db_manager.connection.execute(sql, (start_time, end_time))
            week_tides = {}
            
            for row in result.fetchall():
                tide_time = row[0]
                tide_datetime = datetime.fromtimestamp(tide_time)
                date_key = tide_datetime.strftime('%Y-%m-%d')
                
                if date_key not in week_tides:
                    week_tides[date_key] = {
                        'date': tide_datetime.strftime('%A, %B %d'),
                        'date_short': tide_datetime.strftime('%m/%d'),
                        'is_today': tide_datetime.date() == datetime.now().date(),
                        'tides': []
                    }
                
                week_tides[date_key]['tides'].append({
                    'time': tide_time,
                    'type': 'High' if row[1] == 'H' else 'Low',
                    'height': row[2],
                    'station_id': row[3],
                    'datum': row[4],
                    'days_ahead': row[5],
                    'formatted_time': tide_datetime.strftime('%I:%M %p'),
                    'formatted_height': f"{row[2]:.1f} ft {row[4]}",
                    'is_past': tide_time < time.time()
                })
            
            return week_tides
            
        except Exception as e:
            log.error(f"Error getting week tides: {e}")
        return {}

    def _get_tide_range_today(self, db_manager, current_time):
        """
        IMPLEMENTED: Get today's tide range (high - low)
        
        Args:
            db_manager: Database manager from db_lookup
            current_time: Current time (Unix timestamp)
        """
        try:
            today_tides = self._get_today_tides(db_manager, current_time)
            
            if not today_tides:
                return None
                
            highs = [t['height'] for t in today_tides if t['type'] == 'High']
            lows = [t['height'] for t in today_tides if t['type'] == 'Low']
            
            if highs and lows:
                high_value = max(highs)
                low_value = min(lows)
                tide_range = high_value - low_value
                
                return {
                    'range': tide_range,
                    'high': high_value,
                    'low': low_value,
                    'formatted_range': f"{tide_range:.1f} ft range",
                    'formatted_high': f"{high_value:.1f} ft",
                    'formatted_low': f"{low_value:.1f} ft",
                    'tide_count': len(today_tides)
                }
            
        except Exception as e:
            log.error(f"Error calculating tide range: {e}")
        return None
    
def main():
    """
    FIXED: Command-line interface for testing and debugging
    
    CORRECTIONS:
    - Uses 4-icon standard
    - Clear help text
    - Proper argument handling
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Marine Data Extension Testing and Debugging')
    parser.add_argument('--test-install', action='store_true', help='Test installation only')
    parser.add_argument('--test-api', action='store_true', help='Test API connectivity only')
    parser.add_argument('--test-db', action='store_true', help='Test database operations only')
    parser.add_argument('--test-all', action='store_true', help='Run all tests')
    
    args = parser.parse_args()
    
    if not any([args.test_install, args.test_api, args.test_db, args.test_all]):
        print(f"{CORE_ICONS['navigation']} Marine Data Extension Testing Tool")
        print("Use --help for available options")
        return
    
    tester = MarineDataTester()
    
    if args.test_all:
        tester.run_all_tests()
    elif args.test_install:
        tester.test_installation()
    elif args.test_api:
        tester.test_api_connectivity() 
    elif args.test_db:
        tester.test_database_operations()

if __name__ == '__main__':
    main()