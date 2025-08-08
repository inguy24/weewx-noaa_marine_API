#!/usr/bin/env python3
# Secret Animal: Rooster
"""
WeeWX Marine Data Extension - FUNCTIONAL Core Service

ARCHITECTURAL IMPLEMENTATION:
- USES: WeeWX 5.1 database managers with actual SQL execution
- DATA-DRIVEN: All operations driven by config_dict field_mappings (from installer)
- FUNCTIONAL: Complete API clients with real HTTP requests and data processing
- ENHANCED: tide_table with 7-day rolling predictions and cleanup

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

VERSION = "1.0.0"

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
    """Simple testing class for marine data functionality"""
    
    def __init__(self):
        self.config_dict = None
        self.service_config = None
        self._load_weewx_config()

    def _load_weewx_config(self):
        """Load WeeWX configuration for testing"""
        config_paths = [
            '/etc/weewx/weewx.conf',
            '/home/weewx/weewx.conf',
            os.path.expanduser('~/weewx-data/weewx.conf')
        ]
        
        for config_path in config_paths:
            if os.path.exists(config_path):
                try:
                    import configobj
                    self.config_dict = configobj.ConfigObj(config_path)
                    self.service_config = self.config_dict.get('MarineDataService', {})
                    print(f"{CORE_ICONS['status']} Loaded WeeWX configuration: {config_path}")
                    return
                except Exception as e:
                    continue
        
        print(f"{CORE_ICONS['warning']} No WeeWX configuration found")

    def test_installation(self):
        """Test basic installation components"""
        print(f"\n{CORE_ICONS['selection']} TESTING INSTALLATION")
        print("-" * 40)
        
        if not self.config_dict:
            print(f"{CORE_ICONS['warning']} No WeeWX configuration available")
            return False
        
        success = True
        
        # Check service registration
        print("Checking service registration...")
        engine_config = self.config_dict.get('Engine', {})
        services_config = engine_config.get('Services', {})
        data_services = services_config.get('data_services', '')
        
        if 'user.marine_data.MarineDataService' in str(data_services):
            print(f"  {CORE_ICONS['status']} Service registered in WeeWX configuration")
        else:
            print(f"  {CORE_ICONS['warning']} Service not registered in data_services")
            success = False
        
        # Check service configuration
        print("Checking service configuration...")
        if self.service_config:
            print(f"  {CORE_ICONS['status']} MarineDataService section found")
            
            # Check selected stations
            selected_stations = self.service_config.get('selected_stations', {})
            if selected_stations:
                print(f"  {CORE_ICONS['status']} Station configuration found: {len(selected_stations)} modules")
            else:
                print(f"  {CORE_ICONS['warning']} No station configuration found")
                success = False
            
            # Check field mappings
            field_mappings = self.service_config.get('field_mappings', {})
            if field_mappings:
                print(f"  {CORE_ICONS['status']} Field mappings found: {len(field_mappings)} modules")
            else:
                print(f"  {CORE_ICONS['warning']} No field mappings found")
                success = False
        else:
            print(f"  {CORE_ICONS['warning']} No MarineDataService configuration found")
            success = False
        
        # Check database tables
        print("Checking database tables...")
        try:
            db_tables = self._get_database_tables()
            marine_tables = ['coops_realtime', 'tide_table', 'ndbc_data']
            
            for table in marine_tables:
                if table in db_tables:
                    print(f"  {CORE_ICONS['status']} Table {table} exists")
                else:
                    print(f"  {CORE_ICONS['warning']} Table {table} missing")
                    success = False
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} Error checking database tables: {e}")
            success = False
        
        return success

    def test_api_connectivity(self):
        """Test API connectivity"""
        print(f"\n{CORE_ICONS['navigation']} TESTING API CONNECTIVITY")
        print("-" * 40)
        
        success = True
        
        # Test CO-OPS API
        print("Testing CO-OPS API...")
        try:
            coops_client = COOPSAPIClient(timeout=10)
            test_data = coops_client.get_water_level('9414290')  # La Jolla test station
            
            if test_data and 'data' in test_data:
                print(f"  {CORE_ICONS['status']} CO-OPS API working")
            else:
                print(f"  {CORE_ICONS['warning']} CO-OPS API: No data received")
                success = False
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} CO-OPS API error: {e}")
            success = False
        
        # Test NDBC API
        print("Testing NDBC API...")
        try:
            ndbc_client = NDBCAPIClient(timeout=10)
            test_data = ndbc_client.get_station_data('46042')  # Monterey test station
            
            if test_data and len(test_data) > 0:
                print(f"  {CORE_ICONS['status']} NDBC API working")
            else:
                print(f"  {CORE_ICONS['warning']} NDBC API: No data received")
                success = False
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} NDBC API error: {e}")
            success = False
        
        return success

    def test_database_operations(self):
        """Test database operations"""
        print(f"\n{CORE_ICONS['selection']} TESTING DATABASE OPERATIONS")
        print("-" * 40)
        
        if not self.config_dict:
            print(f"{CORE_ICONS['warning']} No WeeWX configuration available")
            return False
        
        success = True
        
        try:
            # Test WeeWX database manager
            with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as manager:
                print(f"  {CORE_ICONS['status']} WeeWX database manager connection successful")
                
                # Test data insertion
                test_time = int(time.time())
                
                # Test coops_realtime insertion
                coops_sql = """
                    INSERT OR REPLACE INTO coops_realtime 
                    (dateTime, station_id, marine_current_water_level) 
                    VALUES (?, ?, ?)
                """
                manager.connection.execute(coops_sql, (test_time, 'TEST_STATION', 2.5))
                print(f"  {CORE_ICONS['status']} CO-OPS realtime table insertion test passed")
                
                # Test tide_table insertion
                tide_sql = """
                    INSERT OR REPLACE INTO tide_table 
                    (dateTime, station_id, tide_time, tide_type, predicted_height, datum, days_ahead)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                manager.connection.execute(tide_sql, (test_time, 'TEST_STATION', test_time + 3600, 'H', 6.2, 'MLLW', 0))
                print(f"  {CORE_ICONS['status']} Tide table insertion test passed")
                
                # Test ndbc_data insertion
                ndbc_sql = """
                    INSERT OR REPLACE INTO ndbc_data 
                    (dateTime, station_id, marine_wave_height) 
                    VALUES (?, ?, ?)
                """
                manager.connection.execute(ndbc_sql, (test_time, 'TEST_STATION', 3.2))
                print(f"  {CORE_ICONS['status']} NDBC data table insertion test passed")
                
                # Clean up test data
                cleanup_sql = "DELETE FROM {} WHERE station_id = 'TEST_STATION'"
                for table in ['coops_realtime', 'tide_table', 'ndbc_data']:
                    manager.connection.execute(cleanup_sql.format(table))
                print(f"  {CORE_ICONS['status']} Test data cleanup completed")
                
        except Exception as e:
            print(f"  {CORE_ICONS['warning']} Database operation error: {e}")
            success = False
        
        return success

    def run_all_tests(self):
        """Run complete test suite"""
        print(f"\n{CORE_ICONS['selection']} MARINE DATA EXTENSION TESTING")
        print("=" * 60)
        
        tests_passed = 0
        total_tests = 0
        
        # Test installation
        total_tests += 1
        if self.test_installation():
            tests_passed += 1
            print(f"\nInstallation Test: {CORE_ICONS['status']} PASSED")
        else:
            print(f"\nInstallation Test: {CORE_ICONS['warning']} FAILED")
        
        # Test API connectivity
        total_tests += 1
        if self.test_api_connectivity():
            tests_passed += 1
            print(f"\nAPI Connectivity Test: {CORE_ICONS['status']} PASSED")
        else:
            print(f"\nAPI Connectivity Test: {CORE_ICONS['warning']} FAILED")
        
        # Test database operations
        total_tests += 1
        if self.test_database_operations():
            tests_passed += 1
            print(f"\nDatabase Operations Test: {CORE_ICONS['status']} PASSED")
        else:
            print(f"\nDatabase Operations Test: {CORE_ICONS['warning']} FAILED")
        
        # Summary
        print("\n" + "=" * 60)
        print(f"TEST SUMMARY: {tests_passed}/{total_tests} tests passed")
        
        if tests_passed == total_tests:
            print(f"{CORE_ICONS['status']} ALL TESTS PASSED!")
        else:
            print(f"{CORE_ICONS['warning']} SOME TESTS FAILED")
        
        return tests_passed == total_tests

    def _get_database_tables(self):
        """Get list of database tables"""
        with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as manager:
            # Try MySQL first, then SQLite
            try:
                result = manager.connection.execute("SHOW TABLES")
                return [row[0] for row in result.fetchall()]
            except:
                # SQLite
                result = manager.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
                return [row[0] for row in result.fetchall()]

    def main():
        """Command-line interface for testing and debugging"""
        import argparse
        
        parser = argparse.ArgumentParser(description='Marine Data Extension Testing and Debugging')
        parser.add_argument('--test-install', action='store_true', help='Test installation only')
        parser.add_argument('--test-api', action='store_true', help='Test API connectivity only')
        parser.add_argument('--test-db', action='store_true', help='Test database operations only')
        parser.add_argument('--test-all', action='store_true', help='Run all tests')
        
        args = parser.parse_args()
        
        if not any([args.test_install, args.test_api, args.test_db, args.test_all]):
            print("Marine Data Extension - Use --help for options")
            print("Quick test: python3 marine_data.py --test-all")
            return
        
        tester = MarineDataTester()
        
        success = False
        if args.test_all:
            success = tester.run_all_tests()
        elif args.test_install:
            success = tester.test_installation()
        elif args.test_api:
            success = tester.test_api_connectivity()
        elif args.test_db:
            success = tester.test_database_operations()
        
        sys.exit(0 if success else 1)


        if __name__ == '__main__':
            main()
                
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

    def shutDown(self):
        """ITEM 3: Graceful shutdown of background threads"""
        log.info("Marine Data service shutting down...")
        
        # Stop health monitor first
        if hasattr(self, 'health_monitor') and self.health_monitor:
            self.health_monitor.running = False
            try:
                self.health_monitor.join(timeout=5)
                log.info("Health monitor stopped")
            except:
                log.warning("Health monitor did not stop gracefully")
        
        # Stop CO-OPS thread
        if hasattr(self, 'coops_thread') and self.coops_thread:
            self.coops_thread.running = False
            try:
                self.coops_thread.join(timeout=10)
                log.info("CO-OPS background thread stopped")
            except:
                log.warning("CO-OPS thread did not stop gracefully")
        
        # Stop NDBC thread
        if hasattr(self, 'ndbc_thread') and self.ndbc_thread:
            self.ndbc_thread.running = False
            try:
                self.ndbc_thread.join(timeout=10)
                log.info("NDBC background thread stopped")
            except:
                log.warning("NDBC thread did not stop gracefully")
        
        log.info("Marine Data service shutdown complete")

    def _start_health_monitor(self):
        """ITEM 10: Start background thread health monitoring"""
        self.health_monitor = ThreadHealthMonitor(
            service=self,
            check_interval=300  # Check every 5 minutes
        )
        self.health_monitor.daemon = True
        self.health_monitor.start()
        log.info("Thread health monitor started")

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
            enabled_coops = [station_id for station_id, enabled in coops_stations.items() if to_bool(enabled)]
            if enabled_coops:
                stations['coops_module'] = enabled_coops
        
        # Step 3: Get module data - ndbc_stations
        ndbc_stations = selected_stations_config.get('ndbc_stations', {})
        if ndbc_stations:
            enabled_ndbc = [station_id for station_id, enabled in ndbc_stations.items() if to_bool(enabled)]
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

    def determine_target_table(self, database_field):
        """
        SUCCESS MANUAL PATTERN: Determine target table from config_dict field mappings
        """
        # Step 1: Get service section
        service_config = self.config_dict.get('MarineDataService', {})
        
        # Step 2: Get field_mappings subsection
        field_mappings = service_config.get('field_mappings', {})
        
        # Step 3: Search through module mappings
        for module_name, module_fields in field_mappings.items():
            for service_field, field_config in module_fields.items():
                if field_config.get('database_field') == database_field:
                    # Use database_table from field config (set by installer from YAML)
                    return field_config.get('database_table', 'archive')
        
        log.warning(f"Could not determine target table for field: {database_field}")
        return None

    def insert_marine_data(self, station_id, data_record):
        """
        FUNCTIONAL: Insert marine data using WeeWX manager with table routing
        """
        if not self.db_manager or not data_record:
            return False
        
        try:
            # Group fields by target table using config_dict mappings
            table_data = {}
            
            for db_field, value in data_record.items():
                if value is None:
                    continue
                    
                target_table = self.determine_target_table(db_field)
                if target_table and target_table != 'archive':
                    if target_table not in table_data:
                        table_data[target_table] = {}
                    table_data[target_table][db_field] = value
            
            # Insert into each target table
            for table_name, table_fields in table_data.items():
                self._insert_into_table(table_name, station_id, table_fields)
                
            return True
            
        except Exception as e:
            log.error(f"Error inserting marine data: {e}")
            return False

    def _insert_into_table(self, table_name, station_id, field_data):
        """
        FUNCTIONAL: Insert data into specific table using WeeWX manager
        """
        if not field_data:
            return
        
        try:
            if table_name == 'tide_table':
                # Special handling for tide table
                self._insert_tide_table_data(station_id, field_data)
            else:
                # Standard table insertion for coops_realtime and ndbc_data
                self._insert_standard_table_data(table_name, station_id, field_data)
                
        except Exception as e:
            log.error(f"Error inserting into table {table_name}: {e}")

    def _insert_standard_table_data(self, table_name, station_id, field_data):
        """
        FUNCTIONAL: Standard table insertion using WeeWX manager
        """
        # Add common fields
        field_data['dateTime'] = int(time.time())
        field_data['station_id'] = station_id
        
        # Build dynamic SQL
        fields = list(field_data.keys())
        placeholders = ['?'] * len(fields)
        values = list(field_data.values())
        
        sql = f"""
            INSERT OR REPLACE INTO {table_name} 
            ({', '.join(fields)}) 
            VALUES ({', '.join(placeholders)})
        """
        
        # FUNCTIONAL: Actually execute SQL using WeeWX manager
        self.db_manager.connection.execute(sql, values)
        log.debug(f"Inserted data into {table_name} for station {station_id}")

    def _insert_tide_table_data(self, station_id, field_data):
        """
        FUNCTIONAL: Insert tide prediction data into tide_table
        """
        current_time = int(time.time())
        
        # Handle tide predictions data structure
        if 'predictions' in field_data:
            predictions = field_data['predictions']
            for prediction in predictions:
                try:
                    tide_time = self._parse_tide_time(prediction.get('t'))
                    tide_type = prediction.get('type', 'H')
                    height = float(prediction.get('v', 0))
                    datum = prediction.get('datum', 'MLLW')
                    
                    if tide_time:
                        days_ahead = self._calculate_days_ahead(current_time, tide_time)
                        
                        sql = """
                            INSERT OR REPLACE INTO tide_table 
                            (dateTime, station_id, tide_time, tide_type, predicted_height, datum, days_ahead)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        # FUNCTIONAL: Actually execute SQL
                        self.db_manager.connection.execute(sql, (
                            current_time, station_id, tide_time, tide_type, height, datum, days_ahead
                        ))
                        
                except Exception as e:
                    log.error(f"Error inserting tide prediction: {e}")
        
        log.debug(f"Updated tide predictions for station {station_id}")

    def _parse_tide_time(self, time_string):
        """Parse tide time from API response to Unix timestamp"""
        try:
            if isinstance(time_string, str):
                # Parse ISO format time string
                dt = datetime.fromisoformat(time_string.replace('Z', '+00:00'))
                return int(dt.timestamp())
            elif isinstance(time_string, (int, float)):
                return int(time_string)
        except Exception as e:
            log.error(f"Error parsing tide time '{time_string}': {e}")
        return None

    def _calculate_days_ahead(self, current_time, tide_time):
        """Calculate days ahead for tide_table"""
        try:
            current_date = datetime.fromtimestamp(current_time).date()
            tide_date = datetime.fromtimestamp(tide_time).date()
            return (tide_date - current_date).days
        except:
            return 0


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
        """FUNCTIONAL: Collect real-time water level data"""
        for station_id in self.stations:
            try:
                # Get current water level
                data = self.api_client.get_water_level(station_id)
                if data:
                    self._insert_coops_data(station_id, data)
                    
                # Get water temperature if available
                temp_data = self.api_client.get_water_temperature(station_id)
                if temp_data:
                    self._insert_coops_data(station_id, temp_data)
                    
            except Exception as e:
                log.error(f"Error collecting water level for station {station_id}: {e}")

    def _collect_tide_predictions(self):
        """FUNCTIONAL: Collect 7-day tide predictions"""
        for station_id in self.stations:
            try:
                # Get 7 days of predictions
                end_date = datetime.now() + timedelta(days=7)
                data = self.api_client.get_predictions(
                    station_id,
                    begin_date=datetime.now().strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d')
                )
                
                if data and 'predictions' in data:
                    self._insert_tide_predictions(station_id, data)
                    
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
        """FUNCTIONAL: Insert tide predictions using WeeWX manager with database-aware SQL"""
        current_time = int(time.time())
        
        # Clean up old predictions
        cleanup_sql = "DELETE FROM tide_table WHERE station_id = ? AND tide_time < ?"
        yesterday = current_time - 86400
        self.db_manager.connection.execute(cleanup_sql, (station_id, yesterday))
        
        # Insert new predictions
        for prediction in data['predictions']:
            try:
                tide_time_str = prediction.get('t')
                tide_time = int(datetime.fromisoformat(tide_time_str.replace('Z', '+00:00')).timestamp())
                tide_type = prediction.get('type', 'H')
                height = float(prediction.get('v', 0))
                
                # Calculate days ahead
                prediction_date = datetime.fromtimestamp(tide_time)
                current_date = datetime.fromtimestamp(current_time)
                days_ahead = (prediction_date.date() - current_date.date()).days
                
                # Use database-aware upsert SQL
                fields = ['dateTime', 'station_id', 'tide_time', 'tide_type', 'predicted_height', 'datum', 'days_ahead']
                sql = self._get_upsert_sql('tide_table', fields)
                
                # Execute using WeeWX manager
                self.db_manager.connection.execute(sql, (
                    current_time, station_id, tide_time, tide_type, height, 'MLLW', days_ahead
                ))
                
            except Exception as e:
                log.error(f"Error inserting tide prediction: {e}")
        
        log.debug(f"Updated tide predictions for station {station_id}")

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
        """FUNCTIONAL: Make HTTP request with retries"""
        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
        
        for attempt in range(self.retry_attempts):
            try:
                with urllib.request.urlopen(url, timeout=self.timeout) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    
                    # Check for API errors
                    if 'error' in data:
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
    FUNCTIONAL: WeeWX Search List Extension for tide table access
    """
    
    def __init__(self, generator):
        """ITEM 1: Proper SearchList inheritance for WeeWX integration"""
        SearchList.__init__(self, generator)
        self.config_dict = generator.config_dict
        
    def get_extension_list(self, timespan, db_lookup):
        """Provide tide table data for WeeWX templates"""
        search_list = {}
        
        try:
            # Get database manager
            db_manager = self.generator.db_binder.get_manager('wx_binding')
            
            # Get tide information
            search_list['next_high_tide'] = self._get_next_tide(db_manager, 'H')
            search_list['next_low_tide'] = self._get_next_tide(db_manager, 'L')
            search_list['today_tides'] = self._get_today_tides(db_manager)
            search_list['week_tides'] = self._get_week_tides(db_manager)
            
        except Exception as e:
            log.error(f"Error in TideTableSearchList: {e}")
            
        # Return proper format for WeeWX SearchList
        return [search_list]

    def _get_next_tide(self, db_manager, tide_type):
        """Get next high or low tide"""
        try:
            current_time = int(time.time())
            sql = """
                SELECT tide_time, predicted_height, station_id
                FROM tide_table 
                WHERE tide_type = ? AND tide_time > ?
                ORDER BY tide_time LIMIT 1
            """
            
            result = db_manager.connection.execute(sql, (tide_type, current_time))
            row = result.fetchone()
            
            if row:
                return {
                    'time': row[0],
                    'height': row[1],
                    'station_id': row[2],
                    'formatted_time': datetime.fromtimestamp(row[0]).strftime('%I:%M %p'),
                    'formatted_height': f"{row[1]:.1f} ft"
                }

        except Exception as e:
            log.error(f"Error getting next {tide_type} tide: {e}")
        return None