#!/usr/bin/env python3
# Secret Animal: Horse
"""
WeeWX Marine Data Extension - Core Service Framework

Provides NOAA marine data integration with user-selectable stations and fields
following proven OpenWeather extension architectural patterns.

This extension integrates two NOAA data sources:
- CO-OPS (Tides & Currents): Real-time water levels, tide predictions, coastal water temperature
- NDBC (Buoy Data): Offshore marine weather, waves, and sea surface temperature

Architecture follows WeeWX 5.1 StdService patterns with graceful degradation principles.
FIXED: Uses three-table architecture with direct database operations instead of archive injection.

Copyright (C) 2025 Shane Burkhardt
"""

import json
import configobj
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
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple

import weewx
from weewx.engine import StdService
import weewx.units
import weewx.manager
import weeutil.logger

log = weeutil.logger.logging.getLogger(__name__)

VERSION = "1.0.0"

class MarineDataAPIError(Exception):
    """Custom exception for marine data API errors with specific error types."""
    
    def __init__(self, message, error_type=None, station_id=None, api_source=None):
        """
        Initialize marine data API error.
        
        Args:
            message (str): Error description
            error_type (str): Type of error ('rate_limit', 'auth', 'network', etc.)
            station_id (str): Station ID that caused the error
            api_source (str): API source ('coops' or 'ndbc')
        """
        super().__init__(message)
        self.error_type = error_type
        self.station_id = station_id
        self.api_source = api_source


class MarineDatabaseManager:
    """
    Database operations for three-table marine data architecture.
    
    Handles direct insertion into coops_realtime, coops_predictions, 
    and ndbc_data tables using DATA-DRIVEN field mappings from CONF.
    """
    
    def __init__(self, config_dict):
        """
        Initialize marine database manager with configuration.
        
        Args:
            config_dict: WeeWX configuration dictionary
        """
        self.config_dict = config_dict
        self.database_manager = None
        
        # Get field mappings from CONF
        service_config = config_dict.get('MarineDataService', {})
        self.field_mappings = service_config.get('field_mappings', {})
        
        # Initialize database connection
        try:
            self.database_manager = weewx.manager.open_manager_with_config(config_dict, 'wx_binding')
            log.info("Marine database manager initialized")
        except Exception as e:
            log.error(f"Failed to initialize marine database manager: {e}")
            self.database_manager = None
    
    def determine_target_table(self, database_field):
        """
        Determine target table based on CONF field mappings and field analysis.
        
        Args:
            database_field (str): Database field name
            
        Returns:
            str: Target table name ('coops_realtime', 'coops_predictions', 'ndbc_data')
        """
        # Search through CONF field mappings to find this database field
        for module_name, module_fields in self.field_mappings.items():
            for service_field, field_config in module_fields.items():
                if field_config.get('database_field') == database_field:
                    # Use CONF database_table if specified and not 'archive'
                    conf_table = field_config.get('database_table', 'archive')
                    if conf_table != 'archive':
                        return conf_table
                    
                    # Fall back to field analysis for proper routing
                    api_module = module_name.lower()
                    
                    if 'coops' in api_module:
                        # CO-OPS field routing based on field name patterns
                        if any(x in database_field for x in ['current_water_level', 'coastal_water_temp', 'water_level_sigma', 'water_temp_flags']):
                            return 'coops_realtime'
                        elif any(x in database_field for x in ['next_high', 'next_low', 'tide_range']):
                            return 'coops_predictions'
                    elif 'ndbc' in api_module:
                        return 'ndbc_data'
        
        log.warning(f"Could not determine target table for field: {database_field}")
        return None
    
    def insert_marine_data(self, station_id, data_record):
        """
        Insert marine data into appropriate tables using DATA-DRIVEN field mapping.
        
        Args:
            station_id (str): Station identifier
            data_record (dict): Data record with database field names as keys
        """
        if not self.database_manager or not data_record:
            return False
        
        try:
            # Group fields by target table using CONF mappings
            table_data = {}
            
            for db_field, value in data_record.items():
                target_table = self.determine_target_table(db_field)
                if target_table:
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
        Insert data into specific table using dynamic SQL generation.
        
        Args:
            table_name (str): Target table name
            station_id (str): Station identifier  
            field_data (dict): Field data to insert
        """
        if not field_data:
            return
        
        # Add common fields
        field_data['dateTime'] = int(time.time())
        field_data['station_id'] = station_id
        
        # Build dynamic SQL using field mappings
        fields = list(field_data.keys())
        placeholders = ['?' for _ in fields]
        values = [field_data[field] for field in fields]
        
        sql = f"""
        INSERT OR REPLACE INTO {table_name} 
        ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        """
        
        self.database_manager.getSql().execute(sql, values)
        log.debug(f"Inserted data into {table_name} for station {station_id}")
    
    def get_latest_marine_data(self, station_id):
        """Retrieve latest marine data for web interface access."""
        if not self.database_manager:
            return {}
        
        try:
            latest_data = {}
            
            # Get latest from each table
            tables = ['coops_realtime', 'coops_predictions', 'ndbc_data']
            for table in tables:
                try:
                    sql = f"SELECT * FROM {table} WHERE station_id = ? ORDER BY dateTime DESC LIMIT 1"
                    result = self.database_manager.getSql().execute(sql, (station_id,)).fetchone()
                    if result:
                        latest_data[table] = dict(result)
                except Exception:
                    # Table might not exist yet
                    continue
            
            return latest_data
            
        except Exception as e:
            log.error(f"Error retrieving latest marine data for {station_id}: {e}")
            return {}


class MarineDataSearchList:
    """
    Search list extension to provide marine data access for WeeWX reports and templates.
    
    This class enables WeeWX reports to access data from the three marine tables:
    coops_realtime, coops_predictions, and ndbc_data.
    
    Usage in templates: $marine_data.get_latest_data()
    """
    
    def __init__(self, config_dict):
        """
        Initialize marine data search list extension.
        
        Args:
            config_dict: WeeWX configuration dictionary
        """
        self.config_dict = config_dict
        self.db_manager = MarineDatabaseManager(config_dict)
        
        # Get configured stations for data retrieval
        service_config = config_dict.get('MarineDataService', {})
        selected_stations = service_config.get('selected_stations', {})
        
        self.coops_stations = []
        if 'coops_stations' in selected_stations:
            self.coops_stations = [station_id for station_id, enabled in selected_stations['coops_stations'].items() 
                                 if str(enabled).lower() == 'true']
        
        self.ndbc_stations = []
        if 'ndbc_stations' in selected_stations:
            self.ndbc_stations = [station_id for station_id, enabled in selected_stations['ndbc_stations'].items() 
                                if str(enabled).lower() == 'true']
    
    def get_latest_data(self, station_id=None):
        """
        Get latest marine data for report templates.
        
        Args:
            station_id (str, optional): Specific station ID, or None for all stations
            
        Returns:
            dict: Latest marine data organized by station and table
        """
        try:
            if station_id:
                # Get data for specific station
                return self.db_manager.get_latest_marine_data(station_id)
            else:
                # Get data for all configured stations
                all_data = {}
                
                # Get CO-OPS station data
                for station_id in self.coops_stations:
                    station_data = self.db_manager.get_latest_marine_data(station_id)
                    if station_data:
                        all_data[station_id] = station_data
                
                # Get NDBC station data
                for station_id in self.ndbc_stations:
                    station_data = self.db_manager.get_latest_marine_data(station_id)
                    if station_data:
                        all_data[station_id] = station_data
                
                return all_data
                
        except Exception as e:
            log.error(f"Error in marine data search list: {e}")
            return {}
    
    def get_coops_data(self, station_id=None):
        """
        Get CO-OPS specific data (realtime and predictions).
        
        Args:
            station_id (str, optional): Specific CO-OPS station ID
            
        Returns:
            dict: CO-OPS data from coops_realtime and coops_predictions tables
        """
        try:
            if not self.db_manager.database_manager:
                return {}
            
            stations_to_query = [station_id] if station_id else self.coops_stations
            coops_data = {}
            
            for sid in stations_to_query:
                station_data = {}
                
                # Get realtime data
                try:
                    sql = "SELECT * FROM coops_realtime WHERE station_id = ? ORDER BY dateTime DESC LIMIT 1"
                    result = self.db_manager.database_manager.getSql().execute(sql, (sid,)).fetchone()
                    if result:
                        station_data['realtime'] = dict(result)
                except Exception:
                    pass
                
                # Get predictions data
                try:
                    sql = "SELECT * FROM coops_predictions WHERE station_id = ? ORDER BY dateTime DESC LIMIT 1"
                    result = self.db_manager.database_manager.getSql().execute(sql, (sid,)).fetchone()
                    if result:
                        station_data['predictions'] = dict(result)
                except Exception:
                    pass
                
                if station_data:
                    coops_data[sid] = station_data
            
            return coops_data
            
        except Exception as e:
            log.error(f"Error getting CO-OPS data: {e}")
            return {}
    
    def get_ndbc_data(self, station_id=None):
        """
        Get NDBC buoy data.
        
        Args:
            station_id (str, optional): Specific NDBC station ID
            
        Returns:
            dict: NDBC data from ndbc_data table
        """
        try:
            if not self.db_manager.database_manager:
                return {}
            
            stations_to_query = [station_id] if station_id else self.ndbc_stations
            ndbc_data = {}
            
            for sid in stations_to_query:
                try:
                    sql = "SELECT * FROM ndbc_data WHERE station_id = ? ORDER BY dateTime DESC LIMIT 1"
                    result = self.db_manager.database_manager.getSql().execute(sql, (sid,)).fetchone()
                    if result:
                        ndbc_data[sid] = dict(result)
                except Exception:
                    pass
            
            return ndbc_data
            
        except Exception as e:
            log.error(f"Error getting NDBC data: {e}")
            return {}
    
    def get_station_summary(self):
        """
        Get summary of all configured marine stations with latest data availability.
        
        Returns:
            dict: Station summary with data availability status
        """
        try:
            summary = {
                'coops_stations': {},
                'ndbc_stations': {},
                'total_stations': len(self.coops_stations) + len(self.ndbc_stations),
                'data_available': 0
            }
            
            # Check CO-OPS stations
            for station_id in self.coops_stations:
                station_data = self.db_manager.get_latest_marine_data(station_id)
                summary['coops_stations'][station_id] = {
                    'has_realtime': 'coops_realtime' in station_data,
                    'has_predictions': 'coops_predictions' in station_data,
                    'last_update': None
                }
                
                if station_data:
                    summary['data_available'] += 1
                    # Find most recent timestamp
                    timestamps = []
                    for table_data in station_data.values():
                        if 'dateTime' in table_data:
                            timestamps.append(table_data['dateTime'])
                    if timestamps:
                        summary['coops_stations'][station_id]['last_update'] = max(timestamps)
            
            # Check NDBC stations
            for station_id in self.ndbc_stations:
                station_data = self.db_manager.get_latest_marine_data(station_id)
                summary['ndbc_stations'][station_id] = {
                    'has_data': 'ndbc_data' in station_data,
                    'last_update': None
                }
                
                if station_data:
                    summary['data_available'] += 1
                    if 'ndbc_data' in station_data and 'dateTime' in station_data['ndbc_data']:
                        summary['ndbc_stations'][station_id]['last_update'] = station_data['ndbc_data']['dateTime']
            
            return summary
            
        except Exception as e:
            log.error(f"Error getting station summary: {e}")
            return {'coops_stations': {}, 'ndbc_stations': {}, 'total_stations': 0, 'data_available': 0}


class COOPSAPIClient:
    """
    API client for NOAA CO-OPS (Center for Operational Oceanographic Products and Services).
    
    Handles water level observations, tide predictions, and water temperature data
    with proper error handling and rate limiting following CURRENT implementation patterns.
    """
    
    def __init__(self, timeout=30, config_dict=None):
        """
        Initialize CO-OPS API client with DATA-DRIVEN configuration.
        
        Args:
            timeout (int): Request timeout in seconds
            config_dict: WeeWX configuration dictionary for settings
        """
        self.timeout = timeout
        self.config_dict = config_dict
        
        # Get API URL from CONF (NO hardcoding)
        self.base_url = self._get_api_url()
        
        # Rate limiting state
        self.last_request_time = 0
        self.min_request_interval = 5  # Minimum 5 seconds between requests
        
        log.info("CO-OPS API client initialized")
    
    def _get_api_url(self):
        """Get CO-OPS API URL from configuration - DATA DRIVEN."""
        if not self.config_dict:
            return "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        
        service_config = self.config_dict.get('MarineDataService', {})
        coops_config = service_config.get('coops_module', {})
        api_url = coops_config.get('api_url')
        
        if api_url:
            return api_url
        else:
            log.warning("No CO-OPS API URL in configuration, using default")
            return "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

    def _get_station_datum(self, station_id):
        """
        Get the appropriate datum for a specific CO-OPS station from configuration.
        EXACTLY following current implementation.
        """
        if not self.config_dict:
            return 'MLLW'  # Fallback if no config available
            
        service_config = self.config_dict.get('MarineDataService', {})
        coops_config = service_config.get('coops_module', {})
        station_configs = coops_config.get('station_configs', {})
        
        # Get datum for specific station from install.py datum discovery
        station_config = station_configs.get(station_id, {})
        if 'datum' in station_config:
            station_datum = station_config['datum']
            log.debug(f"Using configured datum {station_datum} for CO-OPS station {station_id}")
            return station_datum
        else:
            # Fallback to service-level default (comes from YAML)
            default_datum = coops_config.get('default_datum', 'MLLW')
            log.debug(f"No datum specified for station {station_id}, using default {default_datum}")
            return default_datum

    def collect_water_level(self, station_id, hours_back=1):
        """
        Collect current water level observations from CO-OPS station.
        EXACTLY following current implementation.
        """
        try:
            self._enforce_rate_limit()
            
            # Get station-specific datum from configuration
            datum = self._get_station_datum(station_id)

            # Fix sloppy NOAA endpoint issue with NAVD88 v NAVD depending upon endpoint           
            if datum == 'NAVD88':
                water_level_datum = 'NAVD'
            else:
                water_level_datum = datum

            params = {
                'product': 'water_level',
                'application': 'WeeWX-MarineData',
                'station': station_id,
                'date': 'latest',
                'format': 'json',
                'units': 'english',
                'time_zone': 'gmt',
                'datum': water_level_datum  # Use the API-specific datum
            }

            url = f"{self.base_url}?" + urllib.parse.urlencode(params)
            
            headers = {
                'User-Agent': 'WeeWX-MarineData/1.0',
                'Accept-Encoding': '',  # Override urllib's default 'identity' encoding
                'Accept': '*/*'         # Explicit accept header like curl default
            }
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    raise MarineDataAPIError(f"CO-OPS HTTP error {response.getcode()}: {response.reason}",
                                           error_type='api_error', station_id=station_id, api_source='coops')
                
                data = json.loads(response.read().decode('utf-8'))
                return self._process_water_level_data(data, station_id)
                
        except urllib.error.HTTPError as e:
            # Debug logging for troubleshooting
            if hasattr(e, 'read'):
                try:
                    error_content = e.read().decode('utf-8')
                    log.error(f"DEBUG: CO-OPS API error response: {error_content}")
                except:
                    pass
            
            log.error(f"DEBUG: Water Level Request method: {request.get_method()}")
            log.error(f"DEBUG: Water Level Request headers: {dict(request.headers)}")
            log.error(f"DEBUG: Water Level Request full URL: {request.full_url}")
            
            raise MarineDataAPIError(f"CO-OPS HTTP error {e.code}: {e.reason}",
                                   error_type='api_error', station_id=station_id, api_source='coops')

        except socket.timeout:
            raise MarineDataAPIError("CO-OPS request timeout",
                                   error_type='timeout', station_id=station_id, api_source='coops')
        except json.JSONDecodeError as e:
            raise MarineDataAPIError(f"CO-OPS invalid JSON response: {e}",
                                   error_type='invalid_response', station_id=station_id, api_source='coops')

    def collect_tide_predictions(self, station_id, hours_ahead=48):
        """
        Collect tide predictions from CO-OPS station.
        EXACTLY following current implementation.
        """
        try:
            self._enforce_rate_limit()
            
            # Get station-specific datum from configuration
            datum = self._get_station_datum(station_id)
            
            # Calculate prediction time range using timedelta
            now = datetime.now(timezone.utc)
            end_time = now + timedelta(hours=hours_ahead)
            
            params = {
                'product': 'predictions',
                'application': 'WeeWX-MarineData',  # CRITICAL: Required by CO-OPS API for tracking/rate limiting
                'station': station_id,
                'begin_date': now.strftime('%Y%m%d'),  # Date only, no time
                'end_date': end_time.strftime('%Y%m%d'),  # Date only, no time
                'format': 'json',
                'units': 'english',
                'time_zone': 'gmt',
                'interval': 'hilo',  # High/low tides only
                'datum': datum  # Use station-specific datum
            }
            
            url = f"{self.base_url}?" + urllib.parse.urlencode(params)
            
            headers = {
                'User-Agent': 'WeeWX-MarineData/1.0',
                'Accept-Encoding': '',
                'Accept': '*/*'
            }
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    raise MarineDataAPIError(f"CO-OPS HTTP error {response.getcode()}: {response.reason}",
                                           error_type='api_error', station_id=station_id, api_source='coops')
                
                data = json.loads(response.read().decode('utf-8'))
                return self._process_tide_predictions_data(data, station_id)
                
        except urllib.error.HTTPError as e:
            raise MarineDataAPIError(f"CO-OPS tide predictions error {e.code}: {e.reason}",
                                   error_type='api_error', station_id=station_id, api_source='coops')
        except Exception as e:
            raise MarineDataAPIError(f"Error collecting tide predictions from {station_id}: {e}",
                                   error_type='unknown', station_id=station_id, api_source='coops')

    def collect_water_temperature(self, station_id, hours_back=1):
        """
        Collect water temperature data from CO-OPS station.
        EXACTLY following current implementation.
        """
        try:
            self._enforce_rate_limit()
            
            params = {
                'product': 'water_temperature',
                'application': 'WeeWX-MarineData',  # ADD: Required parameter for API compatibility
                'station': station_id,
                'date': 'latest',
                'format': 'json',
                'units': 'english',
                'time_zone': 'gmt'
            }
            
            url = f"{self.base_url}?" + urllib.parse.urlencode(params)
            
            headers = {
                'User-Agent': 'WeeWX-MarineData/1.0',
                'Accept-Encoding': '',  # Override urllib's default 'identity' encoding
                'Accept': '*/*'         # Explicit accept header like curl default
            }
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    return None  # Water temperature not available at this station
                
                data = json.loads(response.read().decode('utf-8'))
                return self._process_water_temperature_data(data, station_id)
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log.debug(f"Water temperature not available at CO-OPS station {station_id}")
                return None  # Not an error - just not available
            else:
                raise MarineDataAPIError(f"CO-OPS water temperature error {e.code}: {e.reason}",
                                       error_type='api_error', station_id=station_id, api_source='coops')
        except Exception as e:
            log.debug(f"CO-OPS water temperature collection failed for {station_id}: {e}")
            return None  # Non-critical failure

    def _process_water_level_data(self, api_response, station_id):
        """Process CO-OPS water level API response into standardized format."""
        if not api_response or 'data' not in api_response:
            return None
        
        data_points = api_response['data']
        if not data_points:
            return None
        
        # Get most recent data point
        latest = data_points[0]
        
        try:
            processed_data = {
                'marine_current_water_level': float(latest.get('v', 0)),
                'marine_water_level_sigma': float(latest.get('s', 0)),
                'marine_water_level_flags': latest.get('f', '')
            }
            
            return processed_data
            
        except (ValueError, KeyError) as e:
            log.error(f"Error processing water level data for {station_id}: {e}")
            return None

    def _process_tide_predictions_data(self, api_response, station_id):
        """Process CO-OPS tide predictions API response."""
        if not api_response or 'predictions' not in api_response:
            return None
        
        predictions = api_response['predictions']
        if not predictions:
            return None
        
        # Find next high and low tides
        next_high = None
        next_low = None
        
        for pred in predictions:
            if pred.get('type') == 'H' and not next_high:
                next_high = pred
            elif pred.get('type') == 'L' and not next_low:
                next_low = pred
            
            if next_high and next_low:
                break
        
        result = {}
        
        if next_high:
            result.update({
                'marine_next_high_time': next_high.get('t'),
                'marine_next_high_height': float(next_high.get('v', 0))
            })
        
        if next_low:
            result.update({
                'marine_next_low_time': next_low.get('t'),
                'marine_next_low_height': float(next_low.get('v', 0))
            })
        
        if next_high and next_low:
            result['marine_tide_range'] = abs(float(next_high.get('v', 0)) - float(next_low.get('v', 0)))
        
        return result if result else None

    def _process_water_temperature_data(self, api_response, station_id):
        """Process CO-OPS water temperature API response."""
        if not api_response or 'data' not in api_response:
            return None
        
        data_points = api_response['data']
        if not data_points:
            return None
        
        # Get most recent data point
        latest = data_points[0]
        
        try:
            processed_data = {
                'marine_coastal_water_temp': float(latest.get('v', 0)),
                'marine_water_temp_flags': latest.get('f', '')
            }
            
            return processed_data
            
        except (ValueError, KeyError) as e:
            log.error(f"Error processing water temperature data for {station_id}: {e}")
            return None

    def _enforce_rate_limit(self):
        """
        Enforce minimum time between API requests to respect CO-OPS rate limits.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            log.debug(f"Rate limiting: sleeping {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()


class NDBCAPIClient:
    """
    API client for NOAA NDBC (National Data Buoy Center) data files.
    
    Handles parsing of standard meteorological, ocean, and spectral wave data
    from NDBC real-time data files with DATA-DRIVEN configuration.
    """
    
    def __init__(self, timeout=30, config_dict=None):
        """
        Initialize NDBC API client with DATA-DRIVEN configuration.
        
        Args:
            timeout (int): Request timeout in seconds
            config_dict: WeeWX configuration dictionary for settings
        """
        self.timeout = timeout
        self.config_dict = config_dict
        
        # Get API URL from CONF (NO hardcoding)
        self.base_url = self._get_api_url()
        
        log.info("NDBC API client initialized")
    
    def _get_api_url(self):
        """Get NDBC API URL from configuration - DATA DRIVEN."""
        if not self.config_dict:
            return "https://www.ndbc.noaa.gov/data/realtime2"
        
        service_config = self.config_dict.get('MarineDataService', {})
        ndbc_config = service_config.get('ndbc_module', {})
        api_url = ndbc_config.get('api_url')
        
        if api_url:
            return api_url
        else:
            log.warning("No NDBC API URL in configuration, using default")
            return "https://www.ndbc.noaa.gov/data/realtime2"
    
    def collect_standard_met(self, station_id):
        """
        Collect standard meteorological data from NDBC buoy.
        
        Args:
            station_id (str): NDBC station identifier (e.g., '46087')
            
        Returns:
            dict: Processed meteorological data or None if failed
        """
        try:
            file_url = f"{self.base_url}/{station_id}.txt"
            
            with urllib.request.urlopen(file_url, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    raise MarineDataAPIError(f"NDBC returned status {response.getcode()}",
                                           error_type='api_error', station_id=station_id, api_source='ndbc')
                
                content = response.read().decode('utf-8')
                return self._process_stdmet_data(content, station_id)
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise MarineDataAPIError(f"NDBC station {station_id} data not available",
                                       error_type='station_not_found', station_id=station_id, api_source='ndbc')
            else:
                raise MarineDataAPIError(f"NDBC HTTP error {e.code}: {e.reason}",
                                       error_type='api_error', station_id=station_id, api_source='ndbc')
        except urllib.error.URLError as e:
            raise MarineDataAPIError(f"NDBC network error: {e.reason}",
                                   error_type='network_error', station_id=station_id, api_source='ndbc')
        except socket.timeout:
            raise MarineDataAPIError("NDBC request timeout",
                                   error_type='timeout', station_id=station_id, api_source='ndbc')
    
    def collect_ocean_data(self, station_id):
        """
        Collect ocean temperature and salinity data from NDBC buoy.
        
        Args:
            station_id (str): NDBC station identifier
            
        Returns:
            dict: Processed ocean data or None if failed/unavailable
        """
        try:
            file_url = f"{self.base_url}/{station_id}.ocean"
            
            with urllib.request.urlopen(file_url, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    return None  # Ocean data not available for this buoy
                
                content = response.read().decode('utf-8')
                return self._process_ocean_data(content, station_id)
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log.debug(f"Ocean data not available for NDBC station {station_id}")
                return None  # Not an error - just not available
            else:
                raise MarineDataAPIError(f"NDBC ocean data error {e.code}: {e.reason}",
                                       error_type='api_error', station_id=station_id, api_source='ndbc')
        except Exception as e:
            log.debug(f"NDBC ocean data collection failed for {station_id}: {e}")
            return None  # Non-critical failure
    
    def collect_wave_spectra(self, station_id):
        """
        Collect spectral wave data from NDBC buoy.
        
        Args:
            station_id (str): NDBC station identifier
            
        Returns:
            dict: Processed spectral wave data or None if failed/unavailable
        """
        try:
            file_url = f"{self.base_url}/{station_id}.spec"
            
            with urllib.request.urlopen(file_url, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    return None  # Spectral data not available for this buoy
                
                content = response.read().decode('utf-8')
                return self._process_spectral_data(content, station_id)
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log.debug(f"Spectral wave data not available for NDBC station {station_id}")
                return None  # Not an error - just not available
            else:
                raise MarineDataAPIError(f"NDBC spectral data error {e.code}: {e.reason}",
                                       error_type='api_error', station_id=station_id, api_source='ndbc')
        except Exception as e:
            log.debug(f"NDBC spectral data collection failed for {station_id}: {e}")
            return None  # Non-critical failure
    
    def _process_stdmet_data(self, file_content, station_id):
        """
        Process NDBC standard meteorological data file.
        
        Args:
            file_content (str): Raw file content
            station_id (str): Station identifier
            
        Returns:
            dict: Processed meteorological data
        """
        lines = file_content.strip().split('\n')
        
        if len(lines) < 3:
            return None
        
        try:
            headers = lines[0].split()
            latest_data = lines[2].split()
            
            data_dict = dict(zip(headers, latest_data))
            
            processed_data = {}
            
            # Map NDBC fields to our marine fields
            field_mappings = {
                'WVHT': 'marine_wave_height',
                'DPD': 'marine_wave_period', 
                'MWD': 'marine_wave_direction',
                'WSPD': 'marine_wind_speed',
                'WDIR': 'marine_wind_direction',
                'GST': 'marine_wind_gust',
                'ATMP': 'marine_air_temp',
                'WTMP': 'marine_sea_surface_temp',
                'PRES': 'marine_barometric_pressure',
                'VIS': 'marine_visibility',
                'DEWP': 'marine_dewpoint'
            }
            
            for ndbc_field, our_field in field_mappings.items():
                if ndbc_field in data_dict and data_dict[ndbc_field] != 'MM':
                    try:
                        processed_data[our_field] = float(data_dict[ndbc_field])
                    except ValueError:
                        processed_data[our_field] = None
                else:
                    processed_data[our_field] = None
            
            # Add timestamp from NDBC data
            try:
                year = int(data_dict.get('#YY', 0))
                month = int(data_dict.get('MM', 0))
                day = int(data_dict.get('DD', 0))
                hour = int(data_dict.get('hh', 0))
                minute = int(data_dict.get('mm', 0))
                
                # Handle 2-digit year (NDBC format)
                if year < 50:
                    year += 2000
                elif year < 100:
                    year += 1900
                
                timestamp = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                processed_data['timestamp'] = timestamp.isoformat()
                
            except (ValueError, KeyError):
                processed_data['timestamp'] = None
            
            log.debug(f"Processed NDBC standard met data for station {station_id}")
            return processed_data if any(v is not None for v in processed_data.values()) else None
            
        except Exception as e:
            log.error(f"Error processing NDBC standard met data for {station_id}: {e}")
            raise MarineDataAPIError(f"NDBC data processing failed: {e}",
                                   error_type='processing_error', station_id=station_id, api_source='ndbc')
    
    def _process_ocean_data(self, file_content, station_id):
        """
        Process NDBC ocean data file (temperature at depth, salinity, etc.).
        
        Args:
            file_content (str): Raw file content
            station_id (str): Station identifier
            
        Returns:
            dict: Processed ocean data
        """
        lines = file_content.strip().split('\n')
        
        if len(lines) < 3:
            return None
        
        try:
            headers = lines[0].split()
            latest_data = lines[2].split()
            
            data_dict = dict(zip(headers, latest_data))
            
            processed_data = {
                'station_id': station_id,
                'data_type': 'ocean'
            }
            
            # Extract ocean-specific fields
            ocean_mappings = {
                'OTMP': 'ocean_temp_surface',
                'OTMP1': 'ocean_temp_1m',
                'OTMP2': 'ocean_temp_2m',
                'COND': 'conductivity',
                'SAL': 'salinity'
            }
            
            for ndbc_field, our_field in ocean_mappings.items():
                if ndbc_field in data_dict and data_dict[ndbc_field] != 'MM':
                    try:
                        # Trust NOAA QC/QA - only validate for null/missing data
                        processed_data[our_field] = float(data_dict[ndbc_field])
                    except ValueError:
                        # Invalid numeric format
                        processed_data[our_field] = None
                else:
                    # Missing data
                    processed_data[our_field] = None
            
            return processed_data if any(v is not None for k, v in processed_data.items() if k != 'station_id' and k != 'data_type') else None
            
        except Exception as e:
            log.error(f"Error processing NDBC ocean data for {station_id}: {e}")
            return None
    
    def _process_spectral_data(self, file_content, station_id):
        """
        Process NDBC spectral wave data file.
        
        Args:
            file_content (str): Raw file content
            station_id (str): Station identifier
            
        Returns:
            dict: Processed spectral wave data
        """
        # Spectral data processing is complex - simplified for now
        lines = file_content.strip().split('\n')
        
        if len(lines) < 3:
            return None
        
        try:
            # Basic spectral data extraction
            processed_data = {
                'station_id': station_id,
                'data_type': 'spectral_waves',
                'spectral_data_available': True
            }
            
            # In full implementation, would parse frequency bands and energy density
            # For now, just confirm spectral data is available
            
            return processed_data
            
        except Exception as e:
            log.error(f"Error processing NDBC spectral data for {station_id}: {e}")
            return None


class MarineFieldManager:
    """
    Manages marine data field selection and database mapping using configuration data.
    
    Handles field selection validation, database field mapping, and unit group assignment
    following OpenWeather extension patterns but adapted for marine data sources.
    """
    
    def __init__(self, config_dict=None):
        """
        Initialize marine field manager with configuration.
        
        Args:
            config_dict: WeeWX configuration dictionary for field mappings
        """
        self.config_dict = config_dict
        
        log.info("Marine field manager initialized")
    
    def get_database_field_mappings(self, selected_fields):
        """
        Convert field selection to database field mappings using configuration data.
        
        Args:
            selected_fields (dict): Selected fields by module
                
        Returns:
            dict: Database field mappings
        """
        mappings = {}
        
        if not self.config_dict:
            log.error("No configuration data available for field mappings")
            return mappings
            
        service_config = self.config_dict.get('MarineDataService', {})
        if not service_config:
            log.error("No MarineDataService configuration found")
            return mappings
            
        field_mappings = service_config.get('field_mappings', {})
        if not field_mappings:
            log.error("No field_mappings found in service configuration")
            return mappings
        
        # Extract database fields from configuration mappings
        for module_name, field_list in selected_fields.items():
            if isinstance(field_list, list):
                module_mappings = field_mappings.get(module_name, {})
                if not module_mappings:
                    log.error(f"No field mappings found for module '{module_name}'")
                    continue
                    
                for service_field in field_list:
                    field_mapping = module_mappings.get(service_field, {})
                    if not isinstance(field_mapping, dict):
                        log.error(f"Invalid field mapping for {module_name}.{service_field}: {field_mapping}")
                        continue
                        
                    db_field = field_mapping.get('database_field')
                    db_type = field_mapping.get('database_type')
                    
                    if not db_field:
                        log.error(f"No database_field defined for {module_name}.{service_field}")
                        continue
                        
                    if not db_type:
                        log.error(f"No database_type defined for {module_name}.{service_field}")
                        continue
                        
                    mappings[db_field] = db_type
        
        return mappings
    
    def map_service_to_database_field(self, service_field, module_name):
        """
        Map service field name to database field name using configuration data.
        
        Args:
            service_field (str): Service field name (e.g., 'current_water_level')
            module_name (str): Module name (e.g., 'coops_module')
            
        Returns:
            str: Database field name or None if mapping not found
        """
        try:
            if not self.config_dict:
                log.error(f"No configuration data available for field mapping: {service_field}")
                return None
                
            service_config = self.config_dict.get('MarineDataService', {})
            if not service_config:
                log.error(f"No MarineDataService configuration found for field mapping: {service_field}")
                return None
                
            field_mappings = service_config.get('field_mappings', {})
            if not field_mappings:
                log.error(f"No field_mappings found in configuration for field: {service_field}")
                return None
                
            module_mappings = field_mappings.get(module_name, {})
            if not module_mappings:
                log.error(f"No field mappings found for module '{module_name}' and field '{service_field}'")
                return None
                
            field_mapping = module_mappings.get(service_field, {})
            if not isinstance(field_mapping, dict):
                log.error(f"Invalid field mapping for {module_name}.{service_field}: {field_mapping}")
                return None
                
            database_field = field_mapping.get('database_field')
            if not database_field:
                log.error(f"No database_field defined for {module_name}.{service_field}")
                return None
                
            return database_field
            
        except Exception as e:
            log.error(f"Error mapping service field {service_field}: {e}")
            return None


class COOPSBackgroundThread(threading.Thread):
    """
    Background thread for high-frequency CO-OPS data collection.
    
    Handles 10-minute collection intervals for water level observations and water temperature
    with direct database insertion using EXACT current timing patterns.
    """
    
    def __init__(self, config, selected_stations, config_dict=None):
        """
        Initialize CO-OPS background collection thread.
        
        Args:
            config (dict): Service configuration
            selected_stations (list): List of CO-OPS station IDs
            config_dict: WeeWX configuration dictionary
        """
        super(COOPSBackgroundThread, self).__init__(name='COOPSBackgroundThread')
        self.daemon = True
        self.config = config
        self.selected_stations = selected_stations
        self.running = True
        self.config_dict = config_dict
        
        # Initialize API client
        self.api_client = COOPSAPIClient(
            timeout=int(config.get('timeout', 30)),
            config_dict=config_dict
        )
        
        # Initialize database manager for direct insertion
        self.db_manager = MarineDatabaseManager(config_dict)
        
        # Collection intervals (10 minutes for high-frequency data) - EXACT current timing
        self.collection_interval = int(config.get('coops_collection_interval', 600))  # 10 minutes
        
        # Track last collection times per station
        self.last_collection = {station_id: 0 for station_id in selected_stations}
        
        log.info(f"CO-OPS background thread initialized for {len(selected_stations)} stations")
    
    def run(self):
        """
        Main background thread loop for CO-OPS data collection.
        EXACTLY following current implementation with correct sleep times.
        """
        log.info("CO-OPS background thread started")
        
        while self.running:
            try:
                current_time = time.time()
                
                log.debug(f"CO-OPS thread check: {len(self.selected_stations)} stations, running={self.running}")
                
                # Collect from each selected station
                for station_id in self.selected_stations:
                    try:
                        if (current_time - self.last_collection[station_id] >= self.collection_interval):
                            log.debug(f"CO-OPS thread collecting from station {station_id}")
                            self._collect_station_data(station_id)
                            self.last_collection[station_id] = current_time
                        else:
                            time_remaining = self.collection_interval - (current_time - self.last_collection[station_id])
                            log.debug(f"CO-OPS station {station_id}: {time_remaining:.0f}s until next collection")
                    except Exception as e:
                        log.error(f"CRITICAL: Error in CO-OPS collection loop for station {station_id}: {e}")
                        import traceback
                        log.error(f"CRITICAL: Full traceback: {traceback.format_exc()}")
                
                # Sleep for 1 minute before checking again - EXACT current timing
                time.sleep(60)
                
            except Exception as e:
                log.error(f"CRITICAL: Error in CO-OPS background thread main loop: {e}")
                import traceback
                log.error(f"CRITICAL: Full traceback: {traceback.format_exc()}")
                time.sleep(60)  # Reduced from 300 seconds
    
    def _collect_station_data(self, station_id):
        """
        Collect CO-OPS data from a station and insert directly into database.
        
        Args:
            station_id (str): CO-OPS station identifier
        """
        log.debug(f"Collecting CO-OPS data from station {station_id}")
        
        try:
            collected_data = {}
            
            # Collect water level data
            water_level_data = self.api_client.collect_water_level(station_id)
            if water_level_data:
                collected_data.update(water_level_data)
            
            # Collect water temperature data (if available)
            water_temp_data = self.api_client.collect_water_temperature(station_id)
            if water_temp_data:
                collected_data.update(water_temp_data)
            
            # Insert directly into database using DATA-DRIVEN field mapping
            if collected_data:
                self.db_manager.insert_marine_data(station_id, collected_data)
                
                log_success = str(self.config.get('log_success', 'false')).lower() in ('true', 'yes', '1')
                if log_success:
                    log.info(f"Collected and stored CO-OPS data from station {station_id}: {len(collected_data)} fields")
                    
        except MarineDataAPIError as e:
            log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
            if log_errors:
                log.error(f"CO-OPS API error for station {station_id}: {e}")
        except Exception as e:
            log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
            if log_errors:
                log.error(f"Unexpected error collecting CO-OPS data from {station_id}: {e}")
      
    def shutdown(self):
        """
        Shutdown the CO-OPS background thread gracefully.
        """
        log.info("Shutting down CO-OPS background thread")
        self.running = False


class MarineBackgroundThread(threading.Thread):
    """
    Background thread for lower-frequency marine data collection.
    
    Handles hourly NDBC buoy data and 6-hourly tide predictions with coordinated
    scheduling and direct database insertion using EXACT current timing patterns.
    """
    
    def __init__(self, config, selected_stations, config_dict=None):
        """
        Initialize marine background collection thread.
        
        Args:
            config (dict): Service configuration
            selected_stations (dict): Selected stations by type
            config_dict: WeeWX configuration dictionary
        """
        super(MarineBackgroundThread, self).__init__(name='MarineBackgroundThread')
        self.daemon = True
        self.config = config
        self.selected_stations = selected_stations
        self.running = True
        self.config_dict = config_dict
        
        # Initialize API clients
        self.coops_client = COOPSAPIClient(
            timeout=int(config.get('timeout', 30)),
            config_dict=config_dict
        )
        
        self.ndbc_client = NDBCAPIClient(
            timeout=int(config.get('timeout', 30)),
            config_dict=config_dict
        )
        
        # Initialize database manager for direct insertion
        self.db_manager = MarineDatabaseManager(config_dict)
        
        # Collection intervals - EXACT current timing
        self.intervals = {
            'tide_predictions': int(config.get('tide_predictions_interval', 21600)),  # 6 hours
            'ndbc_weather': int(config.get('ndbc_weather_interval', 3600)),           # 1 hour
            'ndbc_ocean': int(config.get('ndbc_ocean_interval', 3600))                # 1 hour
        }
        
        # Track last collection times
        self.last_collection = {
            'tide_predictions': {},
            'ndbc_weather': {},
            'ndbc_ocean': {}
        }
        
        # Initialize last collection times for all stations
        for station_id in selected_stations.get('coops_stations', []):
            self.last_collection['tide_predictions'][station_id] = 0
        
        for station_id in selected_stations.get('ndbc_stations', []):
            self.last_collection['ndbc_weather'][station_id] = 0
            self.last_collection['ndbc_ocean'][station_id] = 0
        
        total_stations = len(selected_stations.get('coops_stations', [])) + len(selected_stations.get('ndbc_stations', []))
        log.info(f"Marine background thread initialized for {total_stations} stations")
    
    def run(self):
        """
        Main background thread loop for marine data collection.
        EXACTLY following current implementation.
        """
        log.info("Marine background thread started")
        
        while self.running:
            try:
                current_time = time.time()
                
                # Collect tide predictions from CO-OPS stations
                self._collect_tide_predictions(current_time)
                
                # Collect NDBC buoy data
                self._collect_ndbc_data(current_time)
                
                # Sleep for 1 minute before checking again - EXACT current timing
                time.sleep(60)
                
            except Exception as e:
                log.error(f"Error in marine background thread: {e}")
                time.sleep(120)  # Sleep 2 minutes on error - EXACT current timing
    
    def _collect_tide_predictions(self, current_time):
        """
        Collect tide predictions from CO-OPS stations.
        
        Args:
            current_time (float): Current timestamp
        """
        for station_id in self.selected_stations.get('coops_stations', []):
            if not self.running:
                break
            
            if current_time - self.last_collection['tide_predictions'].get(station_id, 0) >= self.intervals['tide_predictions']:
                log.debug(f"Collecting tide predictions from station {station_id}")
                
                try:
                    tide_data = self.coops_client.collect_tide_predictions(station_id)
                    if tide_data:
                        # Insert directly into database using DATA-DRIVEN field mapping
                        self.db_manager.insert_marine_data(station_id, tide_data)
                        
                        log_success = str(self.config.get('log_success', 'false')).lower() in ('true', 'yes', '1')
                        if log_success:
                            log.info(f"Collected and stored tide predictions from station {station_id}")
                    
                    self.last_collection['tide_predictions'][station_id] = current_time
                        
                except MarineDataAPIError as e:
                    log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
                    if log_errors:
                        log.error(f"CO-OPS API error collecting tide predictions from {station_id}: {e}")
                except Exception as e:
                    log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
                    if log_errors:
                        log.error(f"Unexpected error collecting tide predictions from {station_id}: {e}")
    
    def _collect_ndbc_data(self, current_time):
        """
        Collect NDBC weather data from stations.
        
        Args:
            current_time (float): Current timestamp
        """
        for station_id in self.selected_stations.get('ndbc_stations', []):
            if not self.running:
                break
            
            # Collect standard meteorological data
            if (current_time - self.last_collection['ndbc_weather'].get(station_id, 0) >= 
                self.intervals['ndbc_weather']):
                
                try:
                    log.debug(f"Collecting NDBC weather data from station {station_id}")
                    
                    weather_data = self.ndbc_client.collect_standard_met(station_id)
                    if weather_data:
                        # Insert directly into database using DATA-DRIVEN field mapping
                        self.db_manager.insert_marine_data(station_id, weather_data)
                        
                        log_success = str(self.config.get('log_success', 'false')).lower() in ('true', 'yes', '1')
                        if log_success:
                            log.info(f"Collected and stored NDBC weather data from station {station_id}")
                    
                    self.last_collection['ndbc_weather'][station_id] = current_time
                    
                except MarineDataAPIError as e:
                    log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
                    if log_errors:
                        log.error(f"NDBC weather data error for station {station_id}: {e}")
                except Exception as e:
                    log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
                    if log_errors:
                        log.error(f"Unexpected error collecting NDBC weather data from {station_id}: {e}")
            
            # Collect ocean data (when available)
            if (current_time - self.last_collection['ndbc_ocean'].get(station_id, 0) >= 
                self.intervals['ndbc_ocean']):
                
                try:
                    ocean_data = self.ndbc_client.collect_ocean_data(station_id)
                    if ocean_data:
                        # Insert directly into database using DATA-DRIVEN field mapping
                        self.db_manager.insert_marine_data(station_id, ocean_data)
                        
                        log.debug(f"Collected and stored NDBC ocean data from station {station_id}")
                    
                    self.last_collection['ndbc_ocean'][station_id] = current_time
                    
                except Exception as e:
                    # Ocean data failures are non-critical (not all buoys have ocean sensors)
                    log.debug(f"NDBC ocean data not available for station {station_id}: {e}")
            
            # Collect wave spectra data (when available) 
            # Note: This is collected at same interval as ocean data for simplicity
            if (current_time - self.last_collection['ndbc_ocean'].get(station_id, 0) >= 
                self.intervals['ndbc_ocean']):
                
                try:
                    spectral_data = self.ndbc_client.collect_wave_spectra(station_id)
                    if spectral_data:
                        # Insert directly into database using DATA-DRIVEN field mapping
                        self.db_manager.insert_marine_data(station_id, spectral_data)
                        
                        log.debug(f"Collected and stored NDBC spectral data from station {station_id}")
                    
                except Exception as e:
                    # Spectral data failures are non-critical (not all buoys have spectra)
                    log.debug(f"NDBC spectral data not available for station {station_id}: {e}")
    
    def get_latest_data(self):
        """
        Get latest collected marine data in thread-safe manner.
        PRESERVED: Still needed for debugging and testing.
        
        Returns:
            dict: Latest data organized by station ID and data type
        """
        # Note: Even with direct database insertion, we may still need this for debugging
        combined_data = {}
        for station_id in self.selected_stations.get('coops_stations', []):
            combined_data[station_id] = f"CO-OPS predictions for {station_id}"
        for station_id in self.selected_stations.get('ndbc_stations', []):
            combined_data[station_id] = f"NDBC data for {station_id}"
        return combined_data
    
    def shutdown(self):
        """
        Shutdown the marine background thread gracefully.
        """
        log.info("Shutting down marine background thread")
        self.running = False


class MarineDataService(StdService):
    """
    Main WeeWX service for marine data collection from NOAA sources.
    
    Integrates CO-OPS (tides & currents) and NDBC (buoy weather) data with WeeWX
    following proven StdService patterns with graceful degradation and never breaking WeeWX.
    
    FIXED: Uses three-table architecture with direct database operations instead of archive injection.
    NO ARCHIVE BINDING - data goes directly to marine tables.
    """
    
    def __init__(self, engine, config_dict):
        """
        Initialize marine data service with configuration validation and setup.
        
        Args:
            engine: WeeWX engine instance
            config_dict: WeeWX configuration dictionary
        """
        super(MarineDataService, self).__init__(engine, config_dict)
        
        log.info(f"Marine Data service version {VERSION} starting")
        
        self.engine = engine
        self.config_dict = config_dict
        
        # Get marine data service configuration
        self.service_config = config_dict.get('MarineDataService', {})
        
        if not self._validate_basic_config():
            log.error("Marine Data service disabled due to configuration issues")
            self.service_enabled = False
            return
        
        # Load station selection from configuration
        self.selected_stations = self._load_station_selection()
        
        if not self.selected_stations or not any(self.selected_stations.values()):
            log.error("No stations selected - marine data collection disabled")
            log.error("HINT: Run 'weectl extension reconfigure MarineData' to configure stations")
            self.service_enabled = False
            return
        
        # Load field selection from configuration
        self.selected_fields = self._load_field_selection()
        
        if not self.selected_fields:
            log.error("No field selection found - service disabled")
            log.error("HINT: Run 'weectl extension reconfigure MarineData' to configure fields")
            self.service_enabled = False
            return
        
        # Validate and clean field selection
        self.active_fields = self._validate_and_clean_selection()
        
        if not self.active_fields:
            log.error("No usable fields found - all fields have issues")
            log.error("Marine Data service disabled - no usable fields available")
            log.error("HINT: Run 'weectl extension reconfigure MarineData' to fix configuration")
            self.service_enabled = False
            return
        
        # Initialize components (following OpenWeather success pattern)
        self._initialize_data_collection()
        self._setup_unit_system()
        
        # CRITICAL: NO BINDING TO NEW_ARCHIVE_RECORD - using direct database operations instead
        # Archive injection completely eliminated per architectural fix
        
        log.info("Marine Data service initialized successfully")
        self.service_enabled = True
    
    def get_marine_data_for_templates(self):
        """
        Provide marine data access for WeeWX templates and reports.
        IMPLEMENTATION: Returns MarineDataSearchList instance for template access.
        
        Returns:
            MarineDataSearchList: Search list extension for marine data access
        """
        return MarineDataSearchList(self.config_dict)
    
    def _validate_basic_config(self):
        """
        Basic service configuration validation for runtime operation.
        
        Returns:
            bool: True if configuration is valid for operation
        """
        if not self.service_config:
            log.error("MarineDataService configuration section not found")
            return False
        
        if not self.service_config.get('enable', '').lower() == 'true':
            log.info("Marine Data service disabled in configuration")
            return False
        
        return True
    
    def _load_station_selection(self):
        """
        Load selected stations from configuration.
        
        Returns:
            dict: Selected stations by type
        """
        selected_stations_config = self.service_config.get('selected_stations', {})
        
        if not selected_stations_config:
            log.error("No selected_stations section found in configuration")
            return {}
        
        selected_stations = {}
        
        # Load CO-OPS stations
        coops_config = selected_stations_config.get('coops_stations', {})
        if coops_config:
            coops_stations = [station_id for station_id, enabled in coops_config.items() 
                             if str(enabled).lower() == 'true']
            if coops_stations:
                selected_stations['coops_stations'] = coops_stations
                log.info(f"Loaded {len(coops_stations)} CO-OPS stations")
        
        # Load NDBC stations
        ndbc_config = selected_stations_config.get('ndbc_stations', {})
        if ndbc_config:
            ndbc_stations = [station_id for station_id, enabled in ndbc_config.items() 
                            if str(enabled).lower() == 'true']
            if ndbc_stations:
                selected_stations['ndbc_stations'] = ndbc_stations
                log.info(f"Loaded {len(ndbc_stations)} NDBC stations")
        
        if not selected_stations:
            log.error("No stations selected in configuration")
            return {}
        
        log.info(f"Loaded station selection: {list(selected_stations.keys())}")
        return selected_stations
    
    def _load_field_selection(self):
        """Extract selected fields from field_mappings."""
        field_mappings = self.service_config.get('field_mappings', {})
        
        if not field_mappings:
            log.error("No field_mappings found in service configuration")
            return {}
        
        # Extract field selection from field mappings
        selected_fields = {}
        
        for module_name, module_fields in field_mappings.items():
            if isinstance(module_fields, dict):
                field_list = list(module_fields.keys())
                if field_list:
                    selected_fields[module_name] = field_list
        
        if not selected_fields:
            log.error("No fields found in field_mappings")
            return {}
        
        log.info(f"Loaded field selection: {selected_fields}")
        return selected_fields
    
    def _validate_and_clean_selection(self):
        """
        Validate field selection against available stations and clean invalid combinations.
        ADAPTED: For three-table architecture instead of archive table.
        
        Returns:
            dict: Validated and cleaned field selection
        """
        if not self.selected_fields:
            log.warning("No field selection available - marine data collection disabled")
            return {}
        
        field_manager = MarineFieldManager(config_dict=self.config_dict)
        active_fields = {}
        total_selected = 0
        
        try:
            # Get expected database fields based on selection  
            expected_fields = field_manager.get_database_field_mappings(self.selected_fields)
            
            if not expected_fields:
                log.warning("No database fields required for current selection")
                return {}
            
            # Check which fields actually exist in marine database tables
            existing_db_fields = self._get_existing_database_fields()
            
            # Validate each module's fields
            for module, fields in self.selected_fields.items():
                if not fields:
                    continue
                    
                if isinstance(fields, list):
                    total_selected += len(fields)
                    active_module_fields = self._validate_module_fields(
                        module, fields, expected_fields, existing_db_fields, field_manager
                    )
                else:
                    log.warning(f"Invalid field selection format for module '{module}': {fields}")
                    continue
                
                if active_module_fields:
                    active_fields[module] = active_module_fields
            
            # Summary logging
            total_active = self._count_active_fields(active_fields)
            if total_active > 0:
                log.info(f"Field validation complete: {total_active}/{total_selected} fields active")
                if total_active < total_selected:
                    log.warning(f"{total_selected - total_active} fields unavailable - see errors above")
                    log.warning("HINT: Run 'weectl extension reconfigure MarineData' to fix field issues")
            else:
                log.error("No usable fields found - all fields have issues")
            
            return active_fields
            
        except Exception as e:
            log.error(f"Field validation failed: {e}")
            return {}
    
    def _validate_module_fields(self, module, fields, expected_fields, existing_db_fields, field_manager):
        """
        Validate fields for a specific module.
        ADAPTED: For three-table architecture instead of archive table.
        
        Args:
            module (str): Module name
            fields (list): List of field names
            expected_fields (dict): Expected database field mappings
            existing_db_fields (list): Existing database fields
            field_manager: Field manager instance
            
        Returns:
            list: Valid field names
        """
        active_fields = []
        
        if not fields:
            return active_fields
        
        for field in fields:
            try:
                # Find the database field name for this logical field
                db_field = field_manager.map_service_to_database_field(field, module)
                
                if db_field is None:
                    log.error(f"Cannot map field '{field}' in module '{module}' - configuration missing")
                    continue
                
                # For marine data with three-table architecture, fields should exist in appropriate tables
                # If they don't exist, it's a configuration issue
                if db_field not in existing_db_fields:
                    log.warning(f"Database field '{db_field}' missing for '{module}.{field}' - will be created")
                    # Note: We still consider the field active and will create it via database manager
                
                # Field is valid and available (or will be created)
                active_fields.append(field)
                
            except Exception as e:
                log.error(f"Error validating field '{field}' in module '{module}': {e}")
                continue
        
        if active_fields:
            log.info(f"Module '{module}': {len(active_fields)}/{len(fields)} fields active")
        else:
            log.warning(f"Module '{module}': no usable fields")
        
        return active_fields
    
    def _get_existing_database_fields(self):
        """
        Get list of existing marine database fields from three marine tables.
        ADAPTED: Checks coops_realtime, coops_predictions, and ndbc_data tables instead of archive.
        
        Returns:
            list: List of existing marine database field names
        """
        try:
            existing_fields = []
            
            # Check all three marine tables for existing fields
            marine_tables = ['coops_realtime', 'coops_predictions', 'ndbc_data']
            
            with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as dbmanager:
                for table_name in marine_tables:
                    try:
                        # Get table schema
                        for column in dbmanager.connection.genSchemaOf(table_name):
                            field_name = column[1]
                            if field_name.startswith('marine_'):  # Marine fields prefix
                                if field_name not in existing_fields:
                                    existing_fields.append(field_name)
                    except Exception:
                        # Table might not exist yet - that's okay
                        continue
            
            return existing_fields
            
        except Exception as e:
            log.warning(f"Could not check existing database fields: {e}")
            return []
    
    def _count_active_fields(self, fields=None):
        """
        Get total count of active marine fields.
        
        Args:
            fields (dict): Field dictionary to count (defaults to self.active_fields)
            
        Returns:
            int: Total number of active fields
        """
        if fields is None:
            fields = self.active_fields if hasattr(self, 'active_fields') else {}
        return sum(len(module_fields) for module_fields in fields.values() if isinstance(module_fields, list))
    
    def _initialize_data_collection(self):
        """
        Initialize background data collection threads with direct database operations.
        """
        self.background_threads = []
        
        try:
            # Initialize CO-OPS background thread for high-frequency data
            if 'coops_stations' in self.selected_stations:
                self.coops_thread = COOPSBackgroundThread(
                    self.service_config,
                    self.selected_stations['coops_stations'],
                    self.config_dict
                )
                self.coops_thread.start()
                self.background_threads.append(self.coops_thread)
                log.info(f"Started CO-OPS background thread for {len(self.selected_stations['coops_stations'])} stations")
            
            # Initialize marine background thread for lower-frequency data
            marine_stations = {}
            if 'coops_stations' in self.selected_stations:
                marine_stations['coops_stations'] = self.selected_stations['coops_stations']
            if 'ndbc_stations' in self.selected_stations:
                marine_stations['ndbc_stations'] = self.selected_stations['ndbc_stations']
            
            if marine_stations:
                self.marine_thread = MarineBackgroundThread(
                    self.service_config,
                    marine_stations,
                    self.config_dict
                )
                self.marine_thread.start()
                self.background_threads.append(self.marine_thread)
                log.info(f"Started marine background thread for {sum(len(stations) for stations in marine_stations.values())} stations")
            
        except Exception as e:
            log.error(f"Error initializing data collection: {e}")
    
    def _setup_unit_system(self):
        """
        Set up unit system configuration for marine data fields.
        """
        try:
            # Initialize field manager for unit handling
            self.field_manager = MarineFieldManager(config_dict=self.config_dict)
            
            log.info("Marine Data unit system configured")
            
        except Exception as e:
            log.error(f"Error setting up unit system: {e}")
    
    def get_field_count(self, fields=None):
        """
        Get total count of active marine fields.
        
        Args:
            fields (dict): Field dictionary to count (defaults to self.active_fields)
            
        Returns:
            int: Total number of active fields
        """
        if fields is None:
            fields = self.active_fields
        return sum(len(module_fields) for module_fields in fields.values() if isinstance(module_fields, list))
    
    def shutDown(self):
        """
        Clean shutdown of marine data service and all background threads.
        """
        try:
            log.info("Shutting down Marine Data service")
            
            # Shutdown all background threads
            if hasattr(self, 'background_threads'):
                for thread in self.background_threads:
                    if thread and hasattr(thread, 'shutdown'):
                        thread.shutdown()
            
            # Shutdown specific threads
            if hasattr(self, 'coops_thread') and self.coops_thread:
                self.coops_thread.shutdown()
            
            if hasattr(self, 'marine_thread') and self.marine_thread:
                self.marine_thread.shutdown()
            
            log.info("Marine Data service shutdown complete")
            
        except Exception as e:
            log.error(f"Error during Marine Data shutdown: {e}")


class MarineDataTester:
    """
    Simple integrated testing framework for Marine Data extension.
    
    Provides installation verification, API connectivity testing, and station validation
    following OpenWeather extension testing patterns.
    """
    
    def __init__(self):
        """
        Initialize marine data tester with configuration loading.
        """
        print(f"Marine Data Extension Tester v{VERSION}")
        print("=" * 60)
        
        # Initialize required data structures
        self.config_dict = None
        self.service_config = None
        
        # Load real WeeWX configuration
        self._load_weewx_config()
        
        # Initialize components if config is available
        if self.config_dict:
            self.service_config = self.config_dict.get('MarineDataService', {})
        else:
            print("⚠️ No WeeWX configuration loaded")
    
    def _load_weewx_config(self):
        """
        Load the actual WeeWX configuration from standard locations.
        """
        config_paths = [
            '/etc/weewx/weewx.conf',
            '/home/weewx/weewx.conf',
            '/usr/share/weewx/weewx.conf'
        ]
        
        for config_path in config_paths:
            try:
                if os.path.exists(config_path):
                    self.config_dict = configobj.ConfigObj(config_path)
                    print(f"✅ Loaded WeeWX configuration from: {config_path}")
                    return
            except Exception as e:
                print(f"⚠️ Error loading config from {config_path}: {e}")
        
        print("⚠️ No WeeWX configuration found in standard locations")


# Main entry point for command-line testing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Marine Data Extension v{VERSION} Testing Framework")
    parser.add_argument('--test-all', action='store_true', help='Run all tests')
    parser.add_argument('--test-api', action='store_true', help='Test API connectivity')
    parser.add_argument('--test-config', action='store_true', help='Test configuration')
    
    args = parser.parse_args()
    
    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(1)
    
    # Initialize tester
    tester = MarineDataTester()
    
    # Run requested tests
    if args.test_all or args.test_config:
        print("\n🔧 CONFIGURATION TEST")
        print("-" * 40)
        if tester.config_dict:
            service_config = tester.config_dict.get('MarineDataService', {})
            if service_config:
                print("✅ MarineDataService configuration found")
                print(f"  • Enabled: {service_config.get('enable', 'false')}")
                print(f"  • Timeout: {service_config.get('timeout', 'not set')}")
                
                field_mappings = service_config.get('field_mappings', {})
                if field_mappings:
                    total_fields = sum(len(fields) for fields in field_mappings.values())
                    print(f"  • Field mappings: {len(field_mappings)} modules, {total_fields} fields")
                else:
                    print("  ⚠️ No field mappings found")
                
                selected_stations = service_config.get('selected_stations', {})
                if selected_stations:
                    total_stations = sum(len(stations) for stations in selected_stations.values())
                    print(f"  • Selected stations: {len(selected_stations)} types, {total_stations} stations")
                else:
                    print("  ⚠️ No selected stations found")
            else:
                print("❌ MarineDataService configuration not found")
        else:
            print("❌ WeeWX configuration not loaded")
    
    if args.test_all or args.test_api:
        print("\n🌐 API CONNECTIVITY TEST")
        print("-" * 40)
        
        # Test CO-OPS API
        try:
            coops_client = COOPSAPIClient(timeout=10, config_dict=tester.config_dict)
            test_data = coops_client.collect_water_level('9410230')  # LA/Long Beach
            if test_data:
                print("✅ CO-OPS API connectivity confirmed")
                print(f"  • Sample data: {list(test_data.keys())}")
            else:
                print("⚠️ CO-OPS API returned no data")
        except Exception as e:
            print(f"❌ CO-OPS API test failed: {e}")
        
        # Test NDBC API
        try:
            ndbc_client = NDBCAPIClient(timeout=10, config_dict=tester.config_dict)
            test_data = ndbc_client.collect_standard_met('46025')  # Santa Monica Bay
            if test_data:
                print("✅ NDBC API connectivity confirmed")
                print(f"  • Sample data: {list(test_data.keys())}")
            else:
                print("⚠️ NDBC API returned no data")
        except Exception as e:
            print(f"❌ NDBC API test failed: {e}")
    
    print(f"\n{'='*60}")
    print("Marine Data Extension testing complete")
    print(f"{'='*60}")