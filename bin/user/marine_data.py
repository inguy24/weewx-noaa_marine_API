#!/usr/bin/env python3
# Secret Animal: Bison
"""
WeeWX Marine Data Extension - Core Service Framework

Provides NOAA marine data integration with user-selectable stations and fields
following proven OpenWeather extension architectural patterns.

This extension integrates two NOAA data sources:
- CO-OPS (Tides & Currents): Real-time water levels, tide predictions, coastal water temperature
- NDBC (Buoy Data): Offshore marine weather, waves, and sea surface temperature

Architecture follows WeeWX 5.1 StdService patterns with graceful degradation principles.

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
from datetime import datetime, timezone
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


class StationManager:
    """
    Manages marine station discovery, distance calculations, and station health monitoring.
    
    Handles both CO-OPS tide stations and NDBC buoy stations with automatic
    distance-based filtering and station availability checking.
    """
    
    def __init__(self, config_dict=None):
        """
        Initialize station manager with configuration.
        
        Args:
            config_dict: WeeWX configuration dictionary for coordinate access
        """
        self.config_dict = config_dict
        self.station_cache = {}
        self.last_cache_update = 0
        self.cache_duration = 3600  # Cache station data for 1 hour
        
        # Get user location from WeeWX configuration
        self.user_latitude, self.user_longitude = self._get_user_location()
        
        log.info(f"StationManager initialized for location: {self.user_latitude}, {self.user_longitude}")
    
    def _get_user_location(self):
        """
        Extract user location from WeeWX configuration.
        
        Returns:
            tuple: (latitude, longitude) as floats
        """
        if not self.config_dict:
            log.warning("No config_dict available for station manager")
            return 0.0, 0.0
        
        try:
            station_config = self.config_dict.get('Station', {})
            latitude = float(station_config.get('latitude', 0.0))
            longitude = float(station_config.get('longitude', 0.0))
            
            if latitude == 0.0 and longitude == 0.0:
                log.warning("Station coordinates not configured in WeeWX")
            
            return latitude, longitude
            
        except (ValueError, KeyError) as e:
            log.error(f"Error reading station coordinates: {e}")
            return 0.0, 0.0
    
    def discover_nearby_stations(self, max_distance_miles=100):
        """
        Discover CO-OPS and NDBC stations within specified distance.
        
        Args:
            max_distance_miles (int): Maximum distance for station search
            
        Returns:
            dict: Nearby stations organized by type
                {
                    'coops': [{'id': '9410230', 'name': 'La Jolla', 'distance': 15.2, ...}],
                    'ndbc': [{'id': '46087', 'name': 'Coastal Buoy', 'distance': 32.1, ...}]
                }
        """
        log.info(f"Discovering marine stations within {max_distance_miles} miles")
        
        try:
            # Discover CO-OPS tide stations
            coops_stations = self._discover_coops_stations(max_distance_miles)
            
            # Discover NDBC buoy stations
            ndbc_stations = self._discover_ndbc_stations(max_distance_miles)
            
            nearby_stations = {
                'coops': coops_stations,
                'ndbc': ndbc_stations
            }
            
            total_found = len(coops_stations) + len(ndbc_stations)
            log.info(f"Station discovery complete: {len(coops_stations)} CO-OPS, {len(ndbc_stations)} NDBC ({total_found} total)")
            
            return nearby_stations
            
        except Exception as e:
            log.error(f"Station discovery failed: {e}")
            return {'coops': [], 'ndbc': []}
    
    def _discover_coops_stations(self, max_distance_miles):
        """
        Discover CO-OPS tide stations within distance range.
        
        Args:
            max_distance_miles (int): Maximum search distance
            
        Returns:
            list: CO-OPS stations with distance and capability information
        """
        try:
            # Fetch CO-OPS station metadata
            stations_url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions"
            
            with urllib.request.urlopen(stations_url, timeout=30) as response:
                if response.getcode() != 200:
                    raise MarineDataAPIError(f"CO-OPS API returned status {response.getcode()}", 
                                           error_type='api_error', api_source='coops')
                
                data = json.loads(response.read().decode('utf-8'))
                all_stations = data.get('stations', [])
            
            # Filter by distance and add capability information
            nearby_stations = []
            for station in all_stations:
                try:
                    station_lat = float(station.get('lat', 0))
                    station_lon = float(station.get('lng', 0))
                    
                    # Calculate distance
                    distance_miles, bearing = self._calculate_distance_and_bearing(
                        self.user_latitude, self.user_longitude,
                        station_lat, station_lon
                    )
                    
                    if distance_miles <= max_distance_miles:
                        station_info = {
                            'id': station.get('id'),
                            'name': station.get('name', 'Unknown'),
                            'state': station.get('state', ''),
                            'latitude': station_lat,
                            'longitude': station_lon,
                            'distance': distance_miles,
                            'bearing': bearing,
                            'products': self._get_coops_station_products(station),
                            'type': 'coops'
                        }
                        nearby_stations.append(station_info)
                        
                except (ValueError, KeyError) as e:
                    log.debug(f"Skipping invalid CO-OPS station: {e}")
                    continue
            
            # Sort by distance
            nearby_stations.sort(key=lambda x: x['distance'])
            
            log.info(f"Found {len(nearby_stations)} CO-OPS stations within {max_distance_miles} miles")
            return nearby_stations[:10]  # Return top 10 closest
            
        except Exception as e:
            log.error(f"CO-OPS station discovery failed: {e}")
            return []
    
    def _discover_ndbc_stations(self, max_distance_miles):
        """
        Discover NDBC buoy stations within distance range.
        
        Args:
            max_distance_miles (int): Maximum search distance
            
        Returns:
            list: NDBC stations with distance and data type information
        """
        try:
            # NDBC station list (simplified - in production would fetch from NDBC)
            # This is a representative sample of major NDBC buoys
            sample_ndbc_stations = [
                {'id': '46087', 'name': 'California Coastal', 'lat': 33.617, 'lon': -119.052},
                {'id': '46025', 'name': 'Santa Monica Bay', 'lat': 33.749, 'lon': -119.053},
                {'id': '46026', 'name': 'San Francisco', 'lat': 37.759, 'lon': -122.833},
                {'id': '46050', 'name': 'Stonewall Bank', 'lat': 44.056, 'lon': -124.526},
                {'id': '46089', 'name': 'Tillamook', 'lat': 45.775, 'lon': -124.006},
                {'id': '41002', 'name': 'South Hatteras', 'lat': 32.382, 'lon': -75.402},
                {'id': '44013', 'name': 'Boston', 'lat': 42.346, 'lon': -70.651},
                {'id': '44017', 'name': 'Montauk Point', 'lat': 40.694, 'lon': -72.048},
                {'id': '45001', 'name': 'North Michigan', 'lat': 45.347, 'lon': -86.273},
                {'id': '45007', 'name': 'Southeast Michigan', 'lat': 42.673, 'lon': -82.425}
            ]
            
            nearby_stations = []
            for station in sample_ndbc_stations:
                try:
                    # Calculate distance
                    distance_miles, bearing = self._calculate_distance_and_bearing(
                        self.user_latitude, self.user_longitude,
                        station['lat'], station['lon']
                    )
                    
                    if distance_miles <= max_distance_miles:
                        station_info = {
                            'id': station['id'],
                            'name': station['name'],
                            'latitude': station['lat'],
                            'longitude': station['lon'],
                            'distance': distance_miles,
                            'bearing': bearing,
                            'data_types': ['stdmet', 'ocean', 'spec'],  # Available data types
                            'water_depth': 'Unknown',  # Would be fetched from NDBC metadata
                            'type': 'ndbc'
                        }
                        nearby_stations.append(station_info)
                        
                except Exception as e:
                    log.debug(f"Skipping invalid NDBC station: {e}")
                    continue
            
            # Sort by distance
            nearby_stations.sort(key=lambda x: x['distance'])
            
            log.info(f"Found {len(nearby_stations)} NDBC stations within {max_distance_miles} miles")
            return nearby_stations[:10]  # Return top 10 closest
            
        except Exception as e:
            log.error(f"NDBC station discovery failed: {e}")
            return []
    
    def _get_coops_station_products(self, station_data):
        """
        Determine available data products for a CO-OPS station.
        
        Args:
            station_data (dict): Station metadata from CO-OPS API
            
        Returns:
            list: Available products ['water_level', 'predictions', 'water_temperature']
        """
        products = ['predictions']  # All tide stations have predictions
        
        # Check for real-time water level capability
        if station_data.get('sensors'):
            sensors = station_data['sensors']
            if 'waterLevels' in sensors or 'wl' in sensors:
                products.append('water_level')
            if 'waterTemps' in sensors or 'wt' in sensors:
                products.append('water_temperature')
        
        return products
    
    def _calculate_distance_and_bearing(self, lat1, lon1, lat2, lon2):
        """
        Calculate great circle distance and bearing between two points using Haversine formula.
        
        Args:
            lat1, lon1 (float): First point coordinates (degrees)
            lat2, lon2 (float): Second point coordinates (degrees)
            
        Returns:
            tuple: (distance_miles, bearing_degrees)
        """
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula for distance
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth's radius in miles
        earth_radius_miles = 3959.0
        distance_miles = earth_radius_miles * c
        
        # Calculate bearing
        y = math.sin(dlon) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) - 
             math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon))
        bearing_rad = math.atan2(y, x)
        bearing_degrees = (math.degrees(bearing_rad) + 360) % 360
        
        return round(distance_miles, 1), round(bearing_degrees, 0)
    
    def validate_station_configuration(self, selected_stations):
        """
        Validate that selected stations are reachable and operational.
        
        Args:
            selected_stations (dict): User-selected stations by type
            
        Returns:
            dict: Validation results with status and recommendations
        """
        validation_results = {
            'valid_stations': [],
            'invalid_stations': [],
            'warnings': [],
            'recommendations': []
        }
        
        log.info("Validating station configuration...")
        
        # Validate CO-OPS stations
        for station_id in selected_stations.get('coops_stations', []):
            try:
                if self._test_coops_station_connectivity(station_id):
                    validation_results['valid_stations'].append({
                        'id': station_id,
                        'type': 'coops',
                        'status': 'operational'
                    })
                else:
                    validation_results['invalid_stations'].append({
                        'id': station_id,
                        'type': 'coops',
                        'status': 'unreachable'
                    })
            except Exception as e:
                log.error(f"Error validating CO-OPS station {station_id}: {e}")
                validation_results['invalid_stations'].append({
                    'id': station_id,
                    'type': 'coops',
                    'status': 'error',
                    'error': str(e)
                })
        
        # Validate NDBC stations
        for station_id in selected_stations.get('ndbc_stations', []):
            try:
                if self._test_ndbc_station_connectivity(station_id):
                    validation_results['valid_stations'].append({
                        'id': station_id,
                        'type': 'ndbc',
                        'status': 'operational'
                    })
                else:
                    validation_results['invalid_stations'].append({
                        'id': station_id,
                        'type': 'ndbc',
                        'status': 'unreachable'
                    })
            except Exception as e:
                log.error(f"Error validating NDBC station {station_id}: {e}")
                validation_results['invalid_stations'].append({
                    'id': station_id,
                    'type': 'ndbc',
                    'status': 'error',
                    'error': str(e)
                })
        
        # Generate recommendations
        total_stations = len(validation_results['valid_stations'])
        if total_stations < 2:
            validation_results['recommendations'].append(
                "Consider selecting multiple stations for backup data sources"
            )
        
        if validation_results['invalid_stations']:
            validation_results['warnings'].append(
                f"{len(validation_results['invalid_stations'])} stations are not operational"
            )
        
        log.info(f"Station validation complete: {total_stations} operational stations")
        return validation_results
    
    def _test_coops_station_connectivity(self, station_id):
        """
        Test connectivity to a specific CO-OPS station.
        
        Args:
            station_id (str): CO-OPS station identifier
            
        Returns:
            bool: True if station is reachable and has recent data
        """
        try:
            # Test with a simple water level request for last hour
            # Use COOPSAPIClient to build proper URL with configuration-driven parameters
            coops_client = COOPSAPIClient(timeout=10, config_dict=self.config_dict)
            
            # Build test parameters using configuration
            service_config = self.config_dict.get('MarineDataService', {}) if self.config_dict else {}
            coops_config = service_config.get('coops_module', {})
            base_url = coops_config.get('api_url', 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter')
            
            params = {
                'product': 'water_level',
                'station': station_id,
                'date': 'latest',
                'format': 'json',
                'units': 'english',
                'time_zone': 'gmt',
                'datum': coops_client._get_station_datum(station_id)  # Use station-specific datum
            }
            
            url = base_url + '?' + urllib.parse.urlencode(params)
            
            with urllib.request.urlopen(url, timeout=10) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    # Check if we got actual data (not just an empty response)
                    return 'data' in data and len(data.get('data', [])) > 0
                else:
                    return False
                    
        except Exception as e:
            log.debug(f"CO-OPS station {station_id} connectivity test failed: {e}")
            return False
    
    def _test_ndbc_station_connectivity(self, station_id):
        """
        Test connectivity to a specific NDBC station.
        
        Args:
            station_id (str): NDBC station identifier
            
        Returns:
            bool: True if station data file is accessible and has recent data
        """
        try:
            # Test access to standard meteorological data file
            url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt"
            
            with urllib.request.urlopen(url, timeout=10) as response:
                if response.getcode() == 200:
                    content = response.read().decode('utf-8')
                    lines = content.strip().split('\n')
                    # Should have header + units + at least one data line
                    return len(lines) >= 3
                else:
                    return False
                    
        except Exception as e:
            log.debug(f"NDBC station {station_id} connectivity test failed: {e}")
            return False


class COOPSAPIClient:
    """
    API client for NOAA CO-OPS (Center for Operational Oceanographic Products and Services).
    
    Handles water level observations, tide predictions, and water temperature data
    with proper error handling and rate limiting.
    """
    
    def __init__(self, timeout=30, config_dict=None):
        """
        Initialize CO-OPS API client.
        
        Args:
            timeout (int): Request timeout in seconds
            config_dict: WeeWX configuration dictionary for settings
        """
        self.timeout = timeout
        self.config_dict = config_dict
        
        self.base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        
        # Rate limiting state
        self.last_request_time = 0
        self.min_request_interval = 5  # Minimum 5 seconds between requests
        
        log.info("CO-OPS API client initialized")

    def _get_station_datum(self, station_id):
        """
        Get the appropriate datum for a specific CO-OPS station from configuration.
        
        REPLACEMENT: Updated to work with install.py datum discovery system.
        Uses per-station datum from configuration with YAML-driven fallbacks.
        
        Args:
            station_id (str): CO-OPS station identifier
            
        Returns:
            str: Datum code for the station (e.g., 'MLLW', 'NAVD88', 'MSL')
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
        """
        try:
            self._enforce_rate_limit()
            
            # Get station-specific datum from configuration
            datum = self._get_station_datum(station_id)
            
            # Build CO-OPS water level API request
            params = {
                'product': 'water_level',
                'application': 'WeeWX-MarineData',  # ADD: Required parameter for API compatibility
                'station': station_id,
                'date': 'latest',
                'format': 'json',
                'units': 'english',
                'time_zone': 'gmt',
                'datum': self._get_station_datum(station_id)
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
                    raise MarineDataAPIError(f"CO-OPS API returned status {response.getcode()}",
                                           error_type='api_error', station_id=station_id, api_source='coops')
                
                data = json.loads(response.read().decode('utf-8'))
                return self._process_water_level_data(data, station_id)
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise MarineDataAPIError(f"CO-OPS station {station_id} not found",
                                       error_type='station_not_found', station_id=station_id, api_source='coops')
            elif e.code == 429:
                raise MarineDataAPIError("CO-OPS rate limit exceeded",
                                       error_type='rate_limit', station_id=station_id, api_source='coops')
            else:
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
        """
        try:
            self._enforce_rate_limit()
            
            # Get station-specific datum from configuration
            datum = self._get_station_datum(station_id)
            
            # Calculate prediction time range using timedelta
            from datetime import timedelta
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
                'datum': self._get_station_datum(station_id)  # Use station-specific datum
            }
            
            url = f"{self.base_url}?" + urllib.parse.urlencode(params)
            
            log.error(f"DEBUG: CO-OPS URL for {station_id}: {url}")

            # Add User-Agent header like curl
            headers = {
                'User-Agent': 'WeeWX-MarineData/1.0',
                'Accept-Encoding': '',  # Override urllib's default 'identity' encoding
                'Accept': '*/*'         # Explicit accept header like curl default
            }
            request = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.getcode() != 200:
                    raise MarineDataAPIError(f"CO-OPS API returned status {response.getcode()}",
                                        error_type='api_error', station_id=station_id, api_source='coops')
                
                data = json.loads(response.read().decode('utf-8'))
                return self._process_tide_predictions(data, station_id)
                
        except urllib.error.HTTPError as e:
            # Enhanced debugging for HTTP errors
            log.error(f"DEBUG: HTTP Error details - Code: {e.code}, Reason: {e.reason}")
            log.error(f"DEBUG: Failed URL: {url}")
            
            # Try to read error response body
            try:
                error_response = e.read().decode('utf-8')
                log.error(f"DEBUG: Error response body: {error_response[:500]}")
            except:
                log.error("DEBUG: Could not read error response body")
            
            if e.code == 404:
                raise MarineDataAPIError(f"CO-OPS station {station_id} predictions not available",
                                    error_type='station_not_found', station_id=station_id, api_source='coops')
            else:
                raise MarineDataAPIError(f"CO-OPS HTTP error {e.code}: {e.reason}",
                                    error_type='api_error', station_id=station_id, api_source='coops')
            
        except Exception as e:
            raise MarineDataAPIError(f"CO-OPS tide prediction error: {e}",
                                    error_type='api_error', station_id=station_id, api_source='coops')
    
    def collect_water_temperature(self, station_id, hours_back=1):
        """
        Collect water temperature data from CO-OPS station (when available).
        
        Args:
            station_id (str): CO-OPS station identifier
            hours_back (int): Hours of data to retrieve
            
        Returns:
            dict: Processed water temperature data or None if failed/unavailable
                {
                    'station_id': '9410230',
                    'water_temperature': 18.5,
                    'flags': 'verified',
                    'timestamp': '2025-01-31T20:42:00Z'
                }
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
        """
        Process CO-OPS water level API response into standardized format.
        
        Args:
            api_response (dict): Raw API response
            station_id (str): Station identifier
            
        Returns:
            dict: Processed water level data
        """
        if not api_response or 'data' not in api_response:
            return None
        
        data_points = api_response['data']
        if not data_points:
            return None
        
        # Get most recent data point
        latest = data_points[0]
        
        try:
            processed_data = {
                'station_id': station_id,
                'data_type': 'water_level',
                'timestamp': latest.get('t'),
                'water_level': float(latest.get('v', 0)),
                'sigma': float(latest.get('s', 0)) if latest.get('s') else None,
                'flags': latest.get('f', ''),
                'quality': latest.get('q', '')
            }
            
            log.debug(f"Processed water level data for station {station_id}: {processed_data['water_level']} ft")
            return processed_data
            
        except (ValueError, KeyError) as e:
            log.error(f"Error processing water level data for {station_id}: {e}")
            return None
    
    def _process_tide_predictions(self, api_response, station_id):
        """
        Process CO-OPS tide prediction API response into next high/low format.
        
        Args:
            api_response (dict): Raw API response
            station_id (str): Station identifier
            
        Returns:
            dict: Processed tide prediction data
        """
        if not api_response or 'predictions' not in api_response:
            return None
        
        predictions = api_response['predictions']
        if not predictions:
            return None
        
        try:
            current_time = datetime.now(timezone.utc)
            next_high = None
            next_low = None
            
            # Find next high and low tides
            for prediction in predictions:
                pred_time_str = prediction.get('t')
                pred_type = prediction.get('type')
                pred_height = float(prediction.get('v', 0))
                
                if not pred_time_str or not pred_type:
                    continue
                
                # Parse prediction time
                try:
                    pred_time = datetime.fromisoformat(pred_time_str.replace(' ', 'T') + '+00:00')
                except ValueError:
                    continue
                
                # Only consider future predictions
                if pred_time <= current_time:
                    continue
                
                # Find next high tide
                if pred_type == 'H' and not next_high:
                    next_high = {
                        'time': pred_time_str,
                        'height': pred_height
                    }
                
                # Find next low tide
                if pred_type == 'L' and not next_low:
                    next_low = {
                        'time': pred_time_str,
                        'height': pred_height
                    }
                
                # Stop when we have both
                if next_high and next_low:
                    break
            
            # Calculate tidal range if we have both high and low
            tidal_range = None
            if next_high and next_low:
                tidal_range = abs(next_high['height'] - next_low['height'])
            
            processed_data = {
                'station_id': station_id,
                'data_type': 'tide_predictions',
                'next_high_time': next_high['time'] if next_high else None,
                'next_high_height': next_high['height'] if next_high else None,
                'next_low_time': next_low['time'] if next_low else None,
                'next_low_height': next_low['height'] if next_low else None,
                'tidal_range': tidal_range
            }
            
            log.debug(f"Processed tide predictions for station {station_id}")
            return processed_data
            
        except (ValueError, KeyError) as e:
            log.error(f"Error processing tide predictions for {station_id}: {e}")
            return None
    
    def _process_water_temperature_data(self, api_response, station_id):
        """
        Process CO-OPS water temperature API response into standardized format.
        
        Args:
            api_response (dict): Raw API response
            station_id (str): Station identifier
            
        Returns:
            dict: Processed water temperature data
        """
        if not api_response or 'data' not in api_response:
            return None
        
        data_points = api_response['data']
        if not data_points:
            return None
        
        # Get most recent data point
        latest = data_points[0]
        
        try:
            processed_data = {
                'station_id': station_id,
                'data_type': 'water_temperature',
                'timestamp': latest.get('t'),
                'water_temperature': float(latest.get('v', 0)),
                'flags': latest.get('f', '')
            }
            
            log.debug(f"Processed water temperature data for station {station_id}: {processed_data['water_temperature']}°F")
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
    from NDBC real-time data files with data validation and quality control.
    """
    
    def __init__(self, timeout=30, config_dict=None):
        """
        Initialize NDBC API client.
        
        Args:
            timeout (int): Request timeout in seconds
            config_dict: WeeWX configuration dictionary for settings
        """
        self.timeout = timeout
        self.config_dict = config_dict
        
        self.base_url = "https://www.ndbc.noaa.gov/data/realtime2"
        
        # NDBC data file types
        self.file_types = {
            'stdmet': '.txt',      # Standard meteorological data
            'ocean': '.ocean',     # Ocean temperature and salinity
            'spec': '.spec'        # Spectral wave data
        }
        
        log.info("NDBC API client initialized")
    
    def collect_standard_met(self, station_id):
        """
        Collect standard meteorological data from NDBC buoy.
        
        Args:
            station_id (str): NDBC station identifier (e.g., '46087')
            
        Returns:
            dict: Processed meteorological data or None if failed
                {
                    'station_id': '46087',
                    'wave_height': 2.1,
                    'wave_period': 8.2,
                    'wave_direction': 270,
                    'wind_speed': 12.5,
                    'wind_direction': 225,
                    'air_temperature': 18.3,
                    'sea_surface_temp': 16.8,
                    'barometric_pressure': 1013.2
                }
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
            raise MarineDataAPIError(f"NDBC file for {station_id} has insufficient data",
                                   error_type='invalid_data', station_id=station_id, api_source='ndbc')
        
        try:
            # Parse header and units lines
            headers = lines[0].split()
            units = lines[1].split()
            
            # Get most recent data line (line 2, index 2)
            latest_data = lines[2].split()
            
            if len(latest_data) != len(headers):
                raise MarineDataAPIError(f"NDBC data format mismatch for {station_id}",
                                       error_type='invalid_data', station_id=station_id, api_source='ndbc')
            
            # Create data mapping
            data_dict = dict(zip(headers, latest_data))
            
            # Extract and process standard fields (trust NOAA QC/QA for data ranges)
            processed_data = {
                'station_id': station_id,
                'data_type': 'stdmet'
            }
            
            # Map NDBC fields to our standardized field names
            field_mappings = {
                'WVHT': ('wave_height', 'meters'),
                'DPD': ('wave_period', 'seconds'),
                'APD': ('avg_wave_period', 'seconds'),
                'MWD': ('wave_direction', 'degrees'),
                'WSPD': ('wind_speed', 'm/s'),
                'WDIR': ('wind_direction', 'degrees'),
                'GST': ('wind_gust', 'm/s'),
                'ATMP': ('air_temperature', 'celsius'),
                'WTMP': ('sea_surface_temp', 'celsius'),
                'PRES': ('barometric_pressure', 'hPa'),
                'VIS': ('visibility', 'nautical_miles'),
                'DEWP': ('dewpoint', 'celsius'),
                'TIDE': ('tide_level', 'feet')
            }
            
            for ndbc_field, (our_field, units) in field_mappings.items():
                if ndbc_field in data_dict:
                    raw_value = data_dict[ndbc_field]
                    
                    # NDBC uses 'MM' for missing data - only validate for null/missing
                    if raw_value != 'MM' and raw_value is not None:
                        try:
                            numeric_value = float(raw_value)
                            processed_data[our_field] = numeric_value
                        except ValueError:
                            # Invalid numeric format - store as None
                            processed_data[our_field] = None
                    else:
                        # Missing data
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
            return processed_data
            
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
    
    # _validate_ndbc_value method removed - trusting NOAA QC/QA processes
    # NOAA is responsible for data quality control, we only check for null/missing values


# MarineDatabaseManager class completely removed
# Database operations moved to install.py following OpenWeather success patterns


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
                {
                    'coops_module': ['current_water_level', 'next_high_time'],
                    'ndbc_module': ['wave_height', 'sea_surface_temp']
                }
                
        Returns:
            dict: Database field mappings
                {
                    'marine_current_water_level': 'REAL',
                    'marine_next_high_time': 'TEXT',
                    'marine_wave_height': 'REAL'
                }
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
    with thread-safe data storage and automatic error recovery.
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
        
        # Initialize API client
        self.api_client = COOPSAPIClient(
            timeout=int(config.get('timeout', 30)),
            config_dict=config_dict
        )
        
        # Thread-safe data storage (in memory only - following OpenWeather pattern)
        self.data_lock = threading.Lock()
        self.latest_data = {}
        
        # Collection intervals (10 minutes for high-frequency data)
        self.collection_interval = int(config.get('coops_collection_interval', 600))  # 10 minutes
        
        # Track last collection times per station
        self.last_collection = {station_id: 0 for station_id in selected_stations}
        
        log.info(f"CO-OPS background thread initialized for {len(selected_stations)} stations")
    
    def run(self):
        """
        Main background thread loop for CO-OPS data collection.
        """
        log.info("CO-OPS background thread started")
        
        while self.running:
            try:
                current_time = time.time()
                
                # Collect from each selected station
                for station_id in self.selected_stations:
                    if (current_time - self.last_collection[station_id] >= self.collection_interval):
                        self._collect_station_data(station_id)
                        self.last_collection[station_id] = current_time
                
                # Sleep for 1 minute before checking again
                time.sleep(60)
                
            except Exception as e:
                log.error(f"Error in CO-OPS background thread: {e}")
                time.sleep(300)  # Sleep 5 minutes on error
    
    def _collect_station_data(self, station_id):
        """
        Collect all available data from a single CO-OPS station.
        
        Args:
            station_id (str): CO-OPS station identifier
        """
        log.debug(f"Collecting CO-OPS data from station {station_id}")
        
        collected_data = []
        
        try:
            # Collect water level data
            water_level_data = self.api_client.collect_water_level(station_id)
            if water_level_data:
                collected_data.append(water_level_data)
            
            # Collect water temperature data (if available)
            water_temp_data = self.api_client.collect_water_temperature(station_id)
            if water_temp_data:
                # Merge with water level data or create separate record
                if water_level_data:
                    water_level_data.update({
                        'water_temperature': water_temp_data.get('water_temperature'),
                        'water_temp_flags': water_temp_data.get('flags', '')
                    })
                else:
                    collected_data.append(water_temp_data)
            
            # Store in thread-safe manner (following OpenWeather pattern)
            if collected_data:
                with self.data_lock:
                    self.latest_data[station_id] = collected_data
                
                log_success = str(self.config.get('log_success', 'false')).lower() in ('true', 'yes', '1')
                if log_success:
                    log.info(f"Collected CO-OPS data from station {station_id}: {len(collected_data)} records")
                    
        except MarineDataAPIError as e:
            log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
            if log_errors:
                log.error(f"CO-OPS API error for station {station_id}: {e}")
        except Exception as e:
            log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
            if log_errors:
                log.error(f"Unexpected error collecting CO-OPS data from {station_id}: {e}")
    
    def get_latest_data(self):
        """
        Get latest collected CO-OPS data in thread-safe manner.
        
        Returns:
            dict: Latest data organized by station ID
        """
        with self.data_lock:
            return self.latest_data.copy()
    
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
    scheduling and thread-safe data storage.
    """
    
    def __init__(self, config, selected_stations, config_dict=None):
        """
        Initialize marine background collection thread.
        
        Args:
            config (dict): Service configuration
            selected_stations (dict): Selected stations by type
                {
                    'coops_stations': ['9410230', '9410580'],
                    'ndbc_stations': ['46087', '46025']
                }
            config_dict: WeeWX configuration dictionary
        """
        super(MarineBackgroundThread, self).__init__(name='MarineBackgroundThread')
        self.daemon = True
        self.config = config
        self.selected_stations = selected_stations
        self.running = True
        
        # Initialize API clients
        self.coops_client = COOPSAPIClient(
            timeout=int(config.get('timeout', 30)),
            config_dict=config_dict
        )
        
        self.ndbc_client = NDBCAPIClient(
            timeout=int(config.get('timeout', 30)),
            config_dict=config_dict
        )
        
        # Thread-safe data storage
        self.data_lock = threading.Lock()
        self.latest_data = {}
        
        # Collection intervals
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
        """
        log.info("Marine background thread started")
        
        while self.running:
            try:
                current_time = time.time()
                
                # Collect tide predictions from CO-OPS stations
                self._collect_tide_predictions(current_time)
                
                # Collect NDBC buoy data
                self._collect_ndbc_data(current_time)
                
                # Sleep for 5 minutes before checking again
                time.sleep(300)
                
            except Exception as e:
                log.error(f"Error in marine background thread: {e}")
                time.sleep(600)  # Sleep 10 minutes on error
    
    def _collect_tide_predictions(self, current_time):
        """
        Collect tide predictions from CO-OPS stations.
        
        Args:
            current_time (float): Current timestamp
        """
        for station_id in self.selected_stations.get('coops_stations', []):
            if (current_time - self.last_collection['tide_predictions'].get(station_id, 0) >= 
                self.intervals['tide_predictions']):
                
                try:
                    log.debug(f"Collecting tide predictions from CO-OPS station {station_id}")
                    
                    tide_data = self.coops_client.collect_tide_predictions(station_id)
                    if tide_data:
                        # Add metadata for database insertion
                        tide_data.update({
                            'station_type': 'coops',
                            'data_type': 'tide_predictions'
                        })
                        
                        # Store in thread-safe manner (following OpenWeather pattern)
                        with self.data_lock:
                            if station_id not in self.latest_data:
                                self.latest_data[station_id] = {}
                            self.latest_data[station_id]['tide_predictions'] = tide_data
                        
                        log_success = str(self.config.get('log_success', 'false')).lower() in ('true', 'yes', '1')
                        if log_success:
                            log.info(f"Collected tide predictions from CO-OPS station {station_id}")
                    
                    self.last_collection['tide_predictions'][station_id] = current_time
                    
                except MarineDataAPIError as e:
                    log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
                    if log_errors:
                        log.error(f"CO-OPS tide prediction error for station {station_id}: {e}")
                except Exception as e:
                    log_errors = str(self.config.get('log_errors', 'true')).lower() in ('true', 'yes', '1')
                    if log_errors:
                        log.error(f"Unexpected error collecting tide predictions from {station_id}: {e}")
    
    def _collect_ndbc_data(self, current_time):
        """
        Collect NDBC buoy data (standard meteorological and ocean data).
        
        Args:
            current_time (float): Current timestamp
        """
        for station_id in self.selected_stations.get('ndbc_stations', []):
            # Collect standard meteorological data
            if (current_time - self.last_collection['ndbc_weather'].get(station_id, 0) >= 
                self.intervals['ndbc_weather']):
                
                try:
                    log.debug(f"Collecting NDBC weather data from station {station_id}")
                    
                    weather_data = self.ndbc_client.collect_standard_met(station_id)
                    if weather_data:
                        # Add metadata for database insertion
                        weather_data.update({
                            'station_type': 'ndbc',
                            'data_type': 'buoy_weather'
                        })
                        
                        # Store in thread-safe manner (following OpenWeather pattern)
                        with self.data_lock:
                            if station_id not in self.latest_data:
                                self.latest_data[station_id] = {}
                            self.latest_data[station_id]['weather'] = weather_data
                        
                        log_success = str(self.config.get('log_success', 'false')).lower() in ('true', 'yes', '1')
                        if log_success:
                            log.info(f"Collected NDBC weather data from station {station_id}")
                    
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
                        # Add metadata for database insertion
                        ocean_data.update({
                            'station_type': 'ndbc',
                            'data_type': 'buoy_ocean'
                        })
                        
                        # Store in thread-safe manner (following OpenWeather pattern)
                        with self.data_lock:
                            if station_id not in self.latest_data:
                                self.latest_data[station_id] = {}
                            self.latest_data[station_id]['ocean'] = ocean_data
                        
                        log.debug(f"Collected NDBC ocean data from station {station_id}")
                    
                    self.last_collection['ndbc_ocean'][station_id] = current_time
                    
                except Exception as e:
                    # Ocean data failures are non-critical (not all buoys have ocean sensors)
                    log.debug(f"NDBC ocean data not available for station {station_id}: {e}")
    
    def get_latest_data(self):
        """
        Get latest collected marine data in thread-safe manner.
        
        Returns:
            dict: Latest data organized by station ID and data type
        """
        with self.data_lock:
            return self.latest_data.copy()
    
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
        
        # Bind to archive events for data injection
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        
        log.info("Marine Data service initialized successfully")
        self.service_enabled = True
    
    def _validate_basic_config(self):
        """
        Basic service configuration validation for runtime operation (simplified).
        
        Returns:
            bool: True if configuration is valid for operation
        """
        if not self.service_config:
            log.error("MarineDataService configuration section not found")
            return False
        
        if not self.service_config.get('enable', '').lower() == 'true':
            log.info("Marine Data service disabled in configuration")
            return False
        
        # Note: Station coordinate validation moved to install.py (installation-time operation)
        # Runtime service assumes coordinates were validated during installation
        
        return True
    
    def _load_station_selection(self):
        """
        Load selected stations from configuration.
        
        Returns:
            dict: Selected stations by type
                {
                    'coops_stations': ['9410230', '9410580'],
                    'ndbc_stations': ['46087', '46025']
                }
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
        """Extract selected fields from field_mappings - no redundancy."""
        field_mappings = self.service_config.get('field_mappings', {})
        
        if not field_mappings:
            log.error("No field_mappings section found in configuration")
            return {}
        
        selected_fields = {}
        for module_name, module_fields in field_mappings.items():
            if isinstance(module_fields, dict):
                selected_fields[module_name] = list(module_fields.keys())
        
        return selected_fields
    
    def _validate_and_clean_selection(self):
        """
        Validate field selection and return only usable fields.
        
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
            
            # Check which fields actually exist in database tables
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
                
                # For marine data, we create tables during installation, so fields should exist
                # If they don't exist, it's a configuration issue
                if db_field not in existing_db_fields:
                    log.warning(f"Database field '{db_field}' missing for '{module}.{field}' - will be created")
                    # Note: Unlike OpenWeather, we still consider the field active and will create it
                
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
        Get list of existing marine database fields from archive table.
        
        Following OpenWeather pattern - marine fields are added to archive table,
        not separate tables. Database schema assumed to exist (created by install.py).
        
        Returns:
            list: List of existing marine database field names
        """
        try:
            db_binding = 'wx_binding'
            existing_fields = []
            
            with weewx.manager.open_manager_with_config(self.config_dict, db_binding) as dbmanager:
                # Check archive table for marine fields (following OpenWeather pattern)
                for column in dbmanager.connection.genSchemaOf('archive'):
                    field_name = column[1]
                    if field_name.startswith('marine_'):
                        existing_fields.append(field_name)
            
            return existing_fields
            
        except Exception as e:
            log.error(f"Error checking database fields: {e}")
            return []
    
    def _initialize_data_collection(self):
        """
        Initialize data collection components following OpenWeather success pattern.
        
        Database schema is assumed to exist (created by install.py).
        Only initializes background threads for data collection.
        """
        try:
            # Initialize field manager for configuration reading
            self.field_manager = MarineFieldManager(config_dict=self.config_dict)
            
            # Initialize background threads based on selected station types
            self.background_threads = []
            
            # Start CO-OPS background thread if CO-OPS stations are selected
            if self.selected_stations.get('coops_stations'):
                self.coops_thread = COOPSBackgroundThread(
                    config=self.service_config,
                    selected_stations=self.selected_stations['coops_stations'],
                    config_dict=self.config_dict
                )
                self.coops_thread.start()
                self.background_threads.append(self.coops_thread)
                log.info("CO-OPS background thread started")
            
            # Start marine background thread for NDBC data and tide predictions
            if (self.selected_stations.get('ndbc_stations') or 
                self.selected_stations.get('coops_stations')):
                self.marine_thread = MarineBackgroundThread(
                    config=self.service_config,
                    selected_stations=self.selected_stations,
                    config_dict=self.config_dict
                )
                self.marine_thread.start()
                self.background_threads.append(self.marine_thread)
                log.info("Marine background thread started")
            
            log.info("Marine data collection initialized successfully")
            self.service_enabled = True
            
        except Exception as e:
            log.error(f"Failed to initialize data collection: {e}")
            log.error("Marine data collection disabled")
            self.service_enabled = False
    
    def _setup_unit_system(self):
        """
        Set up WeeWX unit system integration for marine data fields.
        """
        try:
            import weewx.units
            
            # Get unit system info from configuration
            unit_config = self.service_config.get('unit_system', {})
            weewx_unit_system = unit_config.get('weewx_system', 'US')
            
            log.info(f"Unit system: WeeWX='{weewx_unit_system}' for marine data")
            
            # Add marine-specific unit groups
            if 'group_distance_marine' not in weewx.units.USUnits:
                weewx.units.USUnits['group_distance_marine'] = 'foot'
                weewx.units.MetricUnits['group_distance_marine'] = 'meter'
                weewx.units.MetricWXUnits['group_distance_marine'] = 'meter'
            
            # Read unit groups from configuration field mappings
            conf_field_mappings = self.service_config.get('field_mappings', {})
            
            for module_name, field_list in self.active_fields.items():
                module_mappings = conf_field_mappings.get(module_name, {})
                
                for service_field in field_list:
                    field_mapping = module_mappings.get(service_field, {})
                    
                    if isinstance(field_mapping, dict):
                        db_field = field_mapping.get('database_field', f'marine_{service_field}')
                        unit_group = field_mapping.get('unit_group', 'group_count')
                        
                        # Assign unit group from configuration data
                        weewx.units.obs_group_dict[db_field] = unit_group
            
            log.info("Marine unit system setup completed")
            
        except Exception as e:
            log.error(f"Failed to setup unit system: {e}")
    
    def _count_active_fields(self, fields=None):
        """
        Count total active fields across all modules.
        
        Args:
            fields (dict): Field dictionary to count (defaults to self.active_fields)
            
        Returns:
            int: Total number of active fields
        """
        if fields is None:
            fields = self.active_fields
        return sum(len(module_fields) for module_fields in fields.values() if isinstance(module_fields, list))
    
    def new_archive_record(self, event):
        """
        Inject marine data into WeeWX archive record - never fails, graceful degradation only.
        
        Args:
            event: WeeWX NEW_ARCHIVE_RECORD event containing the record
        """
        if not self.service_enabled:
            return  # Silently skip if service is disabled
        
        try:
            # Get latest collected data from background threads
            collected_data = self.get_latest_data()
            
            if not collected_data:
                return  # No data available
            
            # Build record with expected marine fields, using None for missing data
            record_update = {}
            
            # Get expected database fields from configuration (following OpenWeather pattern)
            expected_fields = self.field_manager.get_database_field_mappings(self.active_fields)
            
            fields_injected = 0
            for db_field, field_type in expected_fields.items():
                # Look for this field in collected data from any station
                field_value = self._find_field_value_in_collected_data(db_field, collected_data)
                
                # Inject the field value (None if not found)
                record_update[db_field] = field_value
                if field_value is not None:
                    fields_injected += 1
            
            # Update the archive record (following OpenWeather pattern)
            event.record.update(record_update)
            
            if fields_injected > 0:
                log.debug(f"Injected marine data: {fields_injected}/{len(expected_fields)} fields")
            else:
                log.debug("No marine data available for injection")
                
        except Exception as e:
            log.error(f"Error injecting marine data: {e}")
            # Never re-raise - would break WeeWX
    
    def _find_field_value_in_collected_data(self, db_field, collected_data):
        """
        Find a specific database field value in collected data from all stations.
        
        Args:
            db_field (str): Database field name to find
            collected_data (dict): All collected data from background threads
            
        Returns:
            Value if found, None otherwise
        """
        # Search through all station data for this field
        for station_data in collected_data.values():
            if isinstance(station_data, dict):
                for data_type, data_records in station_data.items():
                    if isinstance(data_records, dict) and db_field in data_records:
                        return data_records[db_field]
                    elif isinstance(data_records, list):
                        for record in data_records:
                            if isinstance(record, dict) and db_field in record:
                                return record[db_field]
        return None
    
    def get_latest_data(self):
        """
        Get latest collected data from all background threads following OpenWeather pattern.
        
        Returns:
            dict: Combined latest data from all sources stored in memory
        """
        try:
            combined_data = {}
            
            # Get CO-OPS data if available
            if hasattr(self, 'coops_thread') and self.coops_thread:
                coops_data = self.coops_thread.get_latest_data()
                if coops_data:
                    combined_data.update(coops_data)
            
            # Get marine data if available
            if hasattr(self, 'marine_thread') and self.marine_thread:
                marine_data = self.marine_thread.get_latest_data()
                if marine_data:
                    # Merge with existing data
                    for station_id, station_data in marine_data.items():
                        if station_id in combined_data:
                            combined_data[station_id].update(station_data)
                        else:
                            combined_data[station_id] = station_data
            
            return combined_data
            
        except Exception as e:
            log.error(f"Error getting latest marine data: {e}")
            return {}
    
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
        self.latitude = None
        self.longitude = None
        
        print(f"Marine Data Extension Tester v{VERSION}")
        print("=" * 60)
        
        # Initialize required data structures
        self.config_dict = None
        self.service_config = None
        self.station_manager = None
        
        # Load real WeeWX configuration
        self._load_weewx_config()
        
        # Initialize components if config is available
        if self.config_dict:
            self.station_manager = StationManager(config_dict=self.config_dict)
            self.service_config = self.config_dict.get('MarineDataService', {})
            
            # Get station coordinates from WeeWX configuration
            station_config = self.config_dict.get('Station', {})
            self.latitude = float(station_config.get('latitude', 0.0))
            self.longitude = float(station_config.get('longitude', 0.0))
            
            if self.latitude != 0.0 and self.longitude != 0.0:
                print(f"Testing location: {self.latitude}, {self.longitude}")
            else:
                print("⚠️ No station coordinates configured")
        else:
            print("⚠️ No WeeWX configuration loaded")
    
    def _load_weewx_config(self):
        """
        Load the actual WeeWX configuration from standard locations.
        """
        config_paths = [
            '/etc/weewx/weewx.conf',
            '/home/weewx/weewx.conf',
            '/opt/weewx/weewx.conf',
            os.path.expanduser('~/weewx-data/weewx.conf')
        ]
        
        for config_path in config_paths:
            if os.path.exists(config_path):
                try:
                    import configobj
                    self.config_dict = configobj.ConfigObj(config_path)
                    print(f"✓ Loaded WeeWX configuration: {config_path}")
                    return
                except Exception as e:
                    print(f"❌ Error loading {config_path}: {e}")
                    continue
        
        print("❌ No WeeWX configuration found - extension may not be installed")
    
    def test_installation(self):
        """
        Test if marine data extension is properly installed.
        
        Returns:
            bool: True if installation is valid
        """
        print("\n🔧 TESTING INSTALLATION")
        print("-" * 40)
        
        if not self.config_dict:
            print("❌ No WeeWX configuration available")
            return False
        
        success = True
        
        # Check service registration
        print("Checking service registration...")
        try:
            engine_config = self.config_dict.get('Engine', {})
            services_config = engine_config.get('Services', {})
            data_services = services_config.get('data_services', '')
            
            if isinstance(data_services, list):
                data_services = ', '.join(data_services)
            
            if 'user.marine_data.MarineDataService' in data_services:
                print("  ✓ Service registered in WeeWX configuration")
            else:
                print("  ❌ Service not registered in data_services")
                success = False
        except Exception as e:
            print(f"  ❌ Error checking service registration: {e}")
            success = False
        
        # Check MarineDataService configuration
        print("Checking service configuration...")
        if self.service_config:
            print("  ✓ MarineDataService section found")
            
            # Check station selection
            selected_stations = self.service_config.get('selected_stations', {})
            if selected_stations:
                coops_count = len([s for s in selected_stations.get('coops_stations', {}).values() if s])
                ndbc_count = len([s for s in selected_stations.get('ndbc_stations', {}).values() if s])
                print(f"  ✓ Station selection configured: {coops_count} CO-OPS, {ndbc_count} NDBC")
            else:
                print("  ❌ No station selection configured")
                success = False
        else:
            print("  ❌ No MarineDataService configuration found")
            success = False
        
        # Check station coordinates
        print("Checking station coordinates...")
        if self.latitude is not None and self.longitude is not None:
            if self.latitude != 0.0 and self.longitude != 0.0:
                print(f"  ✓ Station coordinates configured: {self.latitude}, {self.longitude}")
            else:
                print("  ❌ Station coordinates are zero - invalid location")
                success = False
        else:
            print("  ❌ No station coordinates found in configuration")
            success = False
        
        # Check database tables (following OpenWeather pattern - marine fields in archive table)
        print("Checking database fields...")
        try:
            db_fields = self._get_database_fields()
            marine_fields = [f for f in db_fields if f.startswith('marine_')]
            
            if marine_fields:
                print(f"  ✓ Found {len(marine_fields)} marine database fields in archive table")
            else:
                print("  ❌ No marine database fields found in archive table")
                success = False
        except Exception as e:
            print(f"  ❌ Error checking database fields: {e}")
            success = False
        
        return success
    
    def test_station_discovery(self):
        """
        Test station discovery functionality.
        
        Returns:
            bool: True if station discovery works
        """
        print("\n🗺️ TESTING STATION DISCOVERY")
        print("-" * 40)
        
        if not self.station_manager:
            print("❌ Station manager not initialized")
            return False
        
        if self.latitude == 0.0 and self.longitude == 0.0:
            print("❌ Invalid station coordinates - cannot test discovery")
            return False
        
        success = True
        
        try:
            print("Discovering nearby stations...")
            nearby_stations = self.station_manager.discover_nearby_stations(max_distance_miles=100)
            
            coops_count = len(nearby_stations.get('coops', []))
            ndbc_count = len(nearby_stations.get('ndbc', []))
            
            if coops_count > 0:
                print(f"  ✓ Found {coops_count} CO-OPS stations within 100 miles")
                # Show closest station
                closest_coops = nearby_stations['coops'][0] if nearby_stations['coops'] else None
                if closest_coops:
                    print(f"    Closest: {closest_coops['name']} ({closest_coops['distance']} miles)")
            else:
                print("  ⚠️ No CO-OPS stations found within 100 miles")
            
            if ndbc_count > 0:
                print(f"  ✓ Found {ndbc_count} NDBC stations within 100 miles")
                # Show closest station
                closest_ndbc = nearby_stations['ndbc'][0] if nearby_stations['ndbc'] else None
                if closest_ndbc:
                    print(f"    Closest: {closest_ndbc['name']} ({closest_ndbc['distance']} miles)")
            else:
                print("  ⚠️ No NDBC stations found within 100 miles")
            
            if coops_count == 0 and ndbc_count == 0:
                print("  ❌ No marine stations found - check location or expand search radius")
                success = False
            
        except Exception as e:
            print(f"  ❌ Station discovery failed: {e}")
            success = False
        
        return success
    
    def test_api_connectivity(self):
        """
        Test API connectivity to configured marine data sources.
        
        Returns:
            bool: True if APIs are accessible
        """
        print("\n🌐 TESTING API CONNECTIVITY")
        print("-" * 40)
        
        if not self.service_config:
            print("❌ No service configuration available")
            return False
        
        success = True
        
        # Test CO-OPS API
        print("Testing CO-OPS API connectivity...")
        try:
            coops_client = COOPSAPIClient(timeout=30, config_dict=self.config_dict)
            
            # Test with a well-known station (La Jolla)
            test_station = '9410230'
            water_level_data = coops_client.collect_water_level(test_station)
            
            if water_level_data and 'water_level' in water_level_data:
                print(f"  ✓ CO-OPS API working: {water_level_data['water_level']:.2f} ft at {test_station}")
            else:
                print("  ❌ CO-OPS API: No valid water level data received")
                success = False
                
        except MarineDataAPIError as e:
            print(f"  ❌ CO-OPS API error: {e}")
            success = False
        except Exception as e:
            print(f"  ❌ CO-OPS API unexpected error: {e}")
            success = False
        
        # Test NDBC API
        print("Testing NDBC API connectivity...")
        try:
            ndbc_client = NDBCAPIClient(timeout=30, config_dict=self.config_dict)
            
            # Test with a well-known buoy (California coastal)
            test_buoy = '46087'
            buoy_data = ndbc_client.collect_standard_met(test_buoy)
            
            if buoy_data and 'wave_height' in buoy_data:
                print(f"  ✓ NDBC API working: {buoy_data['wave_height']:.1f}m waves at {test_buoy}")
            else:
                print("  ❌ NDBC API: No valid buoy data received")
                success = False
                
        except MarineDataAPIError as e:
            print(f"  ❌ NDBC API error: {e}")
            success = False
        except Exception as e:
            print(f"  ❌ NDBC API unexpected error: {e}")
            success = False
        
        return success
    
    def _get_database_fields(self):
        """
        Get list of database fields from archive table (following OpenWeather pattern).
        
        Returns:
            list: List of field names in archive table
        """
        if not self.config_dict:
            return []
        
        try:
            db_binding = 'wx_binding'
            with weewx.manager.open_manager_with_config(self.config_dict, db_binding) as dbmanager:
                fields = []
                for column in dbmanager.connection.genSchemaOf('archive'):
                    fields.append(column[1])
                return fields
        except Exception as e:
            raise Exception(f"Database field access failed: {e}")
    
    def run_basic_tests(self):
        """
        Run essential marine data extension tests.
        
        Returns:
            bool: True if all tests pass
        """
        print(f"\n🧪 RUNNING BASIC MARINE DATA TESTS")
        print("=" * 60)
        print(f"Marine Data Extension v{VERSION}")
        print("=" * 60)
        
        tests_passed = 0
        total_tests = 0
        
        # Test installation
        total_tests += 1
        if self.test_installation():
            tests_passed += 1
            print("\nInstallation Test: ✅ PASSED")
        else:
            print("\nInstallation Test: ❌ FAILED")
        
        # Test station discovery
        total_tests += 1
        if self.test_station_discovery():
            tests_passed += 1
            print("\nStation Discovery Test: ✅ PASSED")
        else:
            print("\nStation Discovery Test: ❌ FAILED")
        
        # Test API connectivity
        total_tests += 1
        if self.test_api_connectivity():
            tests_passed += 1
            print("\nAPI Connectivity Test: ✅ PASSED")
        else:
            print("\nAPI Connectivity Test: ❌ FAILED")
        
        # Summary
        print("\n" + "=" * 60)
        print(f"BASIC TEST SUMMARY: {tests_passed}/{total_tests} tests passed")
        
        if tests_passed == total_tests:
            print("🎉 ALL BASIC TESTS PASSED!")
            print("Marine Data extension is properly installed and ready to use.")
        else:
            print("❌ SOME TESTS FAILED")
            print("Check the output above for specific issues.")
        
        return tests_passed == total_tests


def main():
    """
    Main function for command-line testing of Marine Data extension.
    """
    parser = argparse.ArgumentParser(
        description='Marine Data Extension Testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test installation only
  python3 marine_data.py --test-install
  
  # Test station discovery
  python3 marine_data.py --test-stations
  
  # Test everything
  python3 marine_data.py --test-all
        """
    )
    
    # Test options
    parser.add_argument('--test-install', action='store_true',
                       help='Test installation (database + service registration)')
    parser.add_argument('--test-stations', action='store_true',
                       help='Test station discovery functionality')
    parser.add_argument('--test-api', action='store_true', 
                       help='Test API connectivity')
    parser.add_argument('--test-all', action='store_true',
                       help='Run all basic tests')
    
    args = parser.parse_args()
    
    # Initialize tester
    tester = MarineDataTester()
    
    # Run requested tests
    if args.test_all:
        success = tester.run_basic_tests()
    elif args.test_install:
        success = tester.test_installation()
    elif args.test_stations:
        success = tester.test_station_discovery()
    elif args.test_api:
        success = tester.test_api_connectivity()
    else:
        print("No tests specified. Use --help to see available options.")
        print("\nQuick options:")
        print("  --test-all       # Test installation + stations + API")
        print("  --test-install   # Test installation only")
        print("  --test-stations  # Test station discovery")
        print("  --test-api       # Test API connectivity")
        return
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()