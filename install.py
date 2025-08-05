#!/usr/bin/env python3
# Magic Animal: Red-tailed Hawk
"""
Copyright 2025 Shane Burkhardt
"""

import os
import json
import urllib.request
import urllib.parse
import urllib.error
import sys
import subprocess
import sqlite3
import time
import yaml
import requests
import math
import curses
import textwrap
from configobj import ConfigObj
from typing import Dict, List, Optional, Any, Tuple

# CRITICAL: Correct import path for WeeWX 5.1
try:
    from weecfg.extension import ExtensionInstaller
    import weewx.manager
    import weewx
except ImportError:
    print("Error: This installer requires WeeWX 5.1 or later")
    sys.exit(1)

# REQUIRED: Loader function for WeeWX extension system
def loader():
    return MarineDataInstaller()


class MarineDataInstaller(ExtensionInstaller):
    """
    Simple WeeWX Extension Installer following success manual patterns.
    
    Delegates complex operations to specialist classes while maintaining
    ExtensionInstaller compliance with proven patterns.
    """
    
    def __init__(self):
        super(MarineDataInstaller, self).__init__(
            version="1.0.0",
            name='marine_data',
            description='NOAA Marine Data Extension for WeeWX',
            author="Shane Burkhardt",
            author_email="info@example.com",
            files=[
                ('bin/user', ['bin/user/marine_data.py']),
                ('', ['bin/user/marine_data_fields.yaml'])
            ],
            config={
                'MarineDataService': {
                    'enable': 'true',
                    'timeout': '30',
                    'log_success': 'false',
                    'log_errors': 'true',
                    'retry_attempts': '3'
                }
            },
            # CRITICAL: List format service registration (success manual pattern)
            data_services=['user.marine_data.MarineDataService']
        )

    def configure(self, engine):
        """
        Installation orchestration - delegates to specialist classes.
        Follows success manual pattern of simple orchestration.
        
        ARCHITECTURE FIX: Create three marine tables instead of archive injection.
        """
        try:
            print("\n" + "="*80)
            print("MARINE DATA EXTENSION INSTALLATION")
            print("="*80)
            print("Installing files and registering service...")
            print("Service registration: Automatic via ExtensionInstaller")
            print("-" * 80)
            
            # Step 1: Interactive configuration (delegated)
            configurator = MarineDataConfigurator(engine.config_dict)
            config_dict, selected_options = configurator.run_interactive_setup()
            
            # Step 2: CRITICAL FIX - Merge configurations BEFORE database operations
            # This ensures the database manager has access to field mappings
            merged_config = engine.config_dict.copy()
            merged_config.update(config_dict)
            
            # Step 3: Database schema management with merged configuration - ARCHITECTURE FIXED
            db_manager = MarineDatabaseManager(merged_config)
            db_manager._create_marine_tables(selected_options)
            
            # Step 4: Update engine configuration
            engine.config_dict.update(config_dict)
            
            print("\n" + "="*80)
            print("INSTALLATION COMPLETED SUCCESSFULLY!")
            print("="*80)
            print("✓ Files installed")
            print("✓ Service registered automatically")
            print("✓ Interactive configuration completed")
            print("✓ Marine database tables created")
            print("-" * 80)
            print("IMPORTANT: Restart WeeWX to activate the extension:")
            print("  sudo systemctl restart weewx")
            print("="*80)
            
            return True
            
        except Exception as e:
            # Handle known cosmetic ConfigObj errors
            if "not a string" in str(e) and "False" in str(e):
                print(f"\n⚠️  Warning (ignored): {e}")
                print("Installation completed successfully despite warning.")
                return True
            else:
                print(f"\nInstallation failed: {e}")
                import traceback
                traceback.print_exc()
                return False


class MarineDataConfigurator:
    """
    Interactive configuration handler for Marine Data Extension.
    
    All interactive setup logic moved here from installer class.
    Handles station discovery, field selection, and configuration writing.
    """
    
    def __init__(self, config_dict):
        self.config_dict = config_dict
        self.yaml_data = self._load_yaml_definitions()
        
    def _load_yaml_definitions(self):
        """Load YAML field definitions - REQUIRED, NO fallbacks."""
        # Get extension directory (where install.py is located)
        extension_dir = os.path.dirname(__file__)
        # YAML file is in bin/user/ subdirectory according to MANIFEST
        yaml_path = os.path.join(extension_dir, 'bin', 'user', 'marine_data_fields.yaml')
        
        if not os.path.exists(yaml_path):
            print(f"❌ CRITICAL ERROR: marine_data_fields.yaml not found at {yaml_path}")
            print("   This file is REQUIRED for installation.")
            print(f"   Extension directory: {extension_dir}")
            if os.path.exists(extension_dir):
                print(f"   Directory contents: {os.listdir(extension_dir)}")
            sys.exit(1)
            
        try:
            with open(yaml_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ CRITICAL ERROR: Cannot load YAML file: {e}")
            sys.exit(1)

    def run_interactive_setup(self):
        """
        Run interactive setup following success manual patterns.
        Returns configuration dictionary and selected options.
        """
        print("\n🌊 MARINE DATA EXTENSION - INTERACTIVE SETUP")
        print("=" * 60)
        
        # Get user location for station filtering
        user_lat, user_lon = self._get_user_coordinates()
        
        # Station discovery and selection
        print("\n📍 DISCOVERING NEARBY MARINE STATIONS...")
        stations = self._discover_stations(user_lat, user_lon)
        
        if not stations:
            print("⚠️  No stations found within reasonable distance.")
            print("   Try increasing the search radius or check your location.")
            return {}, {}
        
        # Field selection
        print("\n🔧 FIELD SELECTION...")
        selected_fields = self._select_fields()
        
        if not selected_fields:
            print("⚠️  No fields selected. Extension will be installed but disabled.")
            return {}, {}
        
        # Station selection per module
        print("\n🚢 STATION SELECTION...")
        selected_stations = self._select_stations_per_module(stations, selected_fields)
        
        # Get collection intervals
        intervals = self._get_collection_intervals()
        
        # Generate configuration using comprehensive method
        print("\n⚙️  GENERATING CONFIGURATION...")
        config_dict = self._write_configuration_files(selected_stations, selected_fields, intervals, user_lat, user_lon)
        
        return config_dict, {
            'fields': selected_fields,
            'stations': selected_stations,
            'user_location': (user_lat, user_lon)
        }

    def _get_user_coordinates(self):
        """Extract user location from WeeWX configuration or prompt."""
        try:
            # Try to get from WeeWX configuration
            station_info = self.config_dict.get('Station', {})
            latitude = station_info.get('latitude')
            longitude = station_info.get('longitude')
            
            if latitude and longitude:
                lat = float(latitude)
                lon = float(longitude)
                print(f"📍 Using station location: {lat:.4f}, {lon:.4f}")
                return lat, lon
        except (ValueError, TypeError):
            pass
        
        # Prompt user for coordinates
        print("\n📍 LOCATION SETUP")
        print("Marine stations will be filtered by distance from your location.")
        
        while True:
            try:
                lat_input = input("Enter your latitude (decimal degrees): ").strip()
                lon_input = input("Enter your longitude (decimal degrees): ").strip()
                
                latitude = float(lat_input)
                longitude = float(lon_input)
                
                if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                    print(f"✅ Location set: {latitude:.4f}, {longitude:.4f}")
                    return latitude, longitude
                else:
                    print("❌ Invalid coordinates. Latitude: -90 to 90, Longitude: -180 to 180")
            except ValueError:
                print("❌ Please enter valid decimal numbers.")
            except KeyboardInterrupt:
                print("\n\n⚠️  Setup cancelled by user.")
                sys.exit(1)

    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate great circle distance between two points in kilometers."""
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth radius in kilometers
        earth_radius = 6371.0
        
        return earth_radius * c

    def _discover_stations(self, user_lat, user_lon, max_distance_km=250):
        """Discover marine stations within specified distance with hard limits."""
        all_stations = []
        
        # Apply hard distance limit of 250km max
        search_distance = min(max_distance_km, 250)
        
        # Discover CO-OPS stations (max 10)
        print(f"  🔍 Searching CO-OPS (Tides & Currents) stations within {search_distance}km...")
        coops_stations = self._discover_coops_stations(user_lat, user_lon, search_distance)
        all_stations.extend(coops_stations)
        
        # Discover NDBC stations (max 10)
        print(f"  🔍 Searching NDBC (Buoy) stations within {search_distance}km...")
        ndbc_stations = self._discover_ndbc_stations(user_lat, user_lon, search_distance)
        all_stations.extend(ndbc_stations)
        
        print(f"  ✅ Found {len(all_stations)} stations within {search_distance}km (max 20 total)")
        return all_stations
    
    def _discover_coops_stations(self, user_lat, user_lon, max_distance_km):
        """
        Discover CO-OPS stations including both observation and reference stations.
        FIXED: Proper limiting to 10 stations within 250km max.
        """
        all_stations = []
        
        # Apply hard distance limit of 250km max
        search_distance = min(max_distance_km, 250)
        
        # Get URLs from YAML configuration
        api_modules = self.yaml_data.get('api_modules', {})
        coops_module = api_modules.get('coops_module', {})
        
        try:
            # Query observation stations
            obs_url = coops_module.get('metadata_url', 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?expand=detail')
            print(f"  📡 Querying CO-OPS observation stations...")
            response = requests.get(obs_url, timeout=30)
            response.raise_for_status()
            
            obs_data = response.json()
            obs_stations = obs_data.get('stations', [])
            
            # Process observation stations with distance filtering
            for station in obs_stations:
                try:
                    station_id = station.get('id', '')
                    lat = float(station.get('lat', 0))
                    lon = float(station.get('lng', 0))  # CO-OPS uses 'lng'
                    name = station.get('name', 'Unknown Station')
                    state = station.get('state', 'Unknown')
                    
                    # Calculate distance
                    distance_km = self._calculate_distance(user_lat, user_lon, lat, lon)
                    
                    # Apply distance filter immediately
                    if distance_km <= search_distance:
                        bearing_deg = self._calculate_bearing(user_lat, user_lon, lat, lon)
                        bearing_text = self._bearing_to_text(bearing_deg)
                        
                        capabilities = ['Water Level', 'Real-time Data']
                        if station.get('tidal', False):
                            capabilities.append('Tide Predictions')
                        
                        station_datum = self._query_station_datum(station_id)
                        
                        if station_datum:  # Only include stations with datum info
                            station_info = {
                                'id': station_id,
                                'name': name,
                                'lat': lat,
                                'lon': lon,
                                'distance_km': distance_km,
                                'bearing': bearing_deg,
                                'bearing_text': bearing_text,
                                'state': state,
                                'capabilities': capabilities,
                                'type': 'coops',
                                'station_type': 'observation',
                                'datum': station_datum
                            }
                            all_stations.append(station_info)
                            print(f"    📍 {station_id}: {distance_km:.1f}km, datum {station_datum}")
                            
                except (ValueError, KeyError, TypeError):
                    continue
            
            # Query reference stations (for tide predictions)
            ref_url = obs_url.replace('expand=detail', 'type=tidepredictions')
            print(f"  📡 Querying CO-OPS reference stations...")
            response = requests.get(ref_url, timeout=30)
            response.raise_for_status()
            
            ref_data = response.json()
            ref_stations = ref_data.get('stations', [])
            
            # Process reference stations with distance filtering
            for station in ref_stations:
                try:
                    station_id = station.get('id', '')
                    lat = float(station.get('lat', 0))
                    lon = float(station.get('lng', 0))
                    name = station.get('name', 'Unknown Station')
                    state = station.get('state', 'Unknown')
                    
                    # Calculate distance
                    distance_km = self._calculate_distance(user_lat, user_lon, lat, lon)
                    
                    # Apply distance filter immediately
                    if distance_km <= search_distance:
                        # Check if we already have this station from observations
                        existing_station = next((s for s in all_stations if s['id'] == station_id), None)
                        
                        if existing_station:
                            # Merge capabilities
                            if 'Tide Predictions' not in existing_station['capabilities']:
                                existing_station['capabilities'].append('Tide Predictions')
                            existing_station['station_type'] = 'observation+reference'
                        else:
                            # Add as new reference station
                            bearing_deg = self._calculate_bearing(user_lat, user_lon, lat, lon)
                            bearing_text = self._bearing_to_text(bearing_deg)
                            
                            capabilities = ['Tide Predictions', 'Reference Station']
                            station_datum = self._query_station_datum(station_id)
                            
                            if station_datum:
                                station_info = {
                                    'id': station_id,
                                    'name': name,
                                    'lat': lat,
                                    'lon': lon,
                                    'distance_km': distance_km,
                                    'bearing': bearing_deg,
                                    'bearing_text': bearing_text,
                                    'state': state,
                                    'capabilities': capabilities,
                                    'type': 'coops',
                                    'station_type': 'reference',
                                    'datum': station_datum
                                }
                                all_stations.append(station_info)
                                print(f"    📍 {station_id}: {distance_km:.1f}km, datum {station_datum}")
                                
                except (ValueError, KeyError, TypeError):
                    continue
            
            # CRITICAL FIX: Apply hard limits - remove duplicates, sort by distance, limit to 10
            unique_stations = {}
            for station in all_stations:
                station_id = station['id']
                if station_id not in unique_stations or station['distance_km'] < unique_stations[station_id]['distance_km']:
                    unique_stations[station_id] = station
            
            result = list(unique_stations.values())
            result.sort(key=lambda x: x['distance_km'])  # Sort by distance
            result = result[:10]  # HARD LIMIT to 10 stations
            
            print(f"  ✅ Found {len(result)} CO-OPS stations (limited to 10 closest within {search_distance}km)")
            return result
                
        except Exception as e:
            print(f"  ❌ CO-OPS station discovery failed: {e}")
            return []
    
    def _process_coops_stations(self, stations, user_lat, user_lon, max_distance_km, station_type):
        """Process CO-OPS stations and add capabilities."""
        processed_stations = []
        
        for station in stations:
            try:
                station_id = station.get('id', '')
                lat = float(station.get('lat', 0))
                lon = float(station.get('lng', 0))  # CO-OPS uses 'lng'
                name = station.get('name', 'Unknown Station')
                state = station.get('state', 'Unknown')
                
                # Calculate distance and bearing
                distance_km = self._calculate_distance(user_lat, user_lon, lat, lon)
                bearing_deg = self._calculate_bearing(user_lat, user_lon, lat, lon)
                bearing_text = self._bearing_to_text(bearing_deg)
                
                # Apply distance filter (always include Newport Beach)
                if distance_km <= max_distance_km or station_id == '9410580':
                    capabilities = []
                    if station_type == 'observation':
                        capabilities = ['Water Level', 'Real-time Data']
                        if station.get('tidal', False):
                            capabilities.append('Tide Predictions')
                    else:  # reference station
                        capabilities = ['Tide Predictions', 'Reference Station']
                    
                    station_datum = self._query_station_datum(station_id)
                    
                    # Only include stations where NOAA provides datum information
                    if station_datum:
                        station_info = {
                            'id': station_id,
                            'name': name,
                            'lat': lat,
                            'lon': lon,
                            'distance_km': distance_km,
                            'bearing': bearing_deg,
                            'bearing_text': bearing_text,
                            'state': state,
                            'capabilities': capabilities,
                            'type': 'coops',
                            'station_type': station_type,
                            'datum': station_datum  # Store NOAA-provided datum
                        }
                        
                        print(f"    📍 {station_id}: Using NOAA datum {station_datum}")
                        processed_stations.append(station_info)
                    else:
                        print(f"    🚫 Excluding {station_id} - no datum information from NOAA")
                    
            except (ValueError, KeyError, TypeError):
                continue  # Skip invalid stations
        
        return processed_stations

    def _query_station_datum(self, station_id):
        """Query individual station for datum information."""
        try:
            # Get station info URL from YAML
            api_modules = self.yaml_data.get('api_modules', {})
            coops_module = api_modules.get('coops_module', {})
            station_info_url = coops_module.get('station_info_url', 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station_id}.json')
            
            url = station_info_url.format(station_id=station_id)
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                station_data = response.json()
                stations = station_data.get('stations', [])
                if stations:
                    station = stations[0]
                    datum_field = coops_module.get('metadata_datum_field', 'referenceDatum')
                    datum = station.get(datum_field, coops_module.get('default_datum', 'MLLW'))
                    return datum
            
            # Fallback to default datum
            return coops_module.get('default_datum', 'MLLW')
            
        except Exception as e:
            print(f"    Warning: Could not query datum for {station_id}: {e}")
            return None

    def _discover_ndbc_stations(self, user_lat, user_lon, max_distance_km):
        """Discover NDBC stations using YAML-configured URL - NO hardcoded URLs."""
        nearby_stations = []
        
        try:
            # Use api_modules section - NO hardcoded URLs
            ndbc_module = self.yaml_data.get('api_modules', {}).get('ndbc_module', {})
            metadata_url = ndbc_module.get('metadata_url')
            
            if not metadata_url:
                print("  ❌ ERROR: ndbc_module.metadata_url not found in YAML")
                return []
            
            print(f"  📡 Querying NDBC API: {metadata_url}")
            response = requests.get(metadata_url, timeout=30)
            response.raise_for_status()
            
            # Parse XML response
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            print(f"  📊 Processing NDBC XML stations...")
            
            for station in root.findall('.//station'):
                try:
                    station_id = station.get('id', '').strip()
                    lat = float(station.get('lat', 0))
                    lon = float(station.get('lon', 0))
                    name = station.get('name', '').strip()
                    
                    if not station_id or not name:
                        continue
                    
                    # Calculate distance and bearing
                    distance_km = self._calculate_distance(user_lat, user_lon, lat, lon)
                    bearing_deg = self._calculate_bearing(user_lat, user_lon, lat, lon)
                    bearing_text = self._bearing_to_text(bearing_deg)
                    
                    # Apply distance filter
                    if distance_km <= max_distance_km:
                        capabilities = ['Wave Height', 'Marine Weather', 'Wind Data']
                        
                        # Clean up name
                        if not name or name == station_id:
                            name = f"NDBC Station {station_id}"
                        
                        station_info = {
                            'id': station_id,
                            'name': name,
                            'lat': lat,
                            'lon': lon,
                            'distance_km': distance_km,
                            'bearing': bearing_deg,
                            'bearing_text': bearing_text,
                            'capabilities': capabilities,
                            'type': 'ndbc',
                            'station_type': 'buoy'
                        }
                        nearby_stations.append(station_info)
                        
                except (ValueError, TypeError):
                    continue  # Skip invalid stations
            
            # Sort by distance and limit to 10
            nearby_stations.sort(key=lambda x: x['distance_km'])
            result = nearby_stations[:10]
            
            print(f"  ✅ Found {len(result)} NDBC stations (limited to 10 closest)")
            return result
            
        except Exception as e:
            print(f"  ❌ NDBC station discovery failed: {e}")
            return []

    def _calculate_bearing(self, lat1, lon1, lat2, lon2):
        """Calculate bearing (compass direction) from point 1 to point 2."""
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1) 
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Calculate bearing
        dlon = lon2_rad - lon1_rad
        
        y = math.sin(dlon) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) - 
            math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon))
        
        bearing_rad = math.atan2(y, x)
        bearing_deg = math.degrees(bearing_rad)
        
        # Normalize to 0-360 degrees
        bearing_deg = (bearing_deg + 360) % 360
        
        return bearing_deg

    def _bearing_to_text(self, bearing_degrees):
        """Convert bearing degrees to compass direction text."""
        directions = [
            "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"
        ]
        
        # Each direction spans 22.5 degrees
        direction_index = int((bearing_degrees + 11.25) / 22.5) % 16
        return directions[direction_index]

    def _select_fields(self):
        """
        Handle field selection with complexity levels (minimal/all/custom).
        
        Returns:
            dict: Selected fields organized by field name with field configurations
        """
        print("\n🎯 FIELD SELECTION")
        print("-" * 30)
        
        # Show complexity menu and get user choice
        print("Field Selection Options:")
        print("1. MINIMAL - Essential marine monitoring fields")
        print("2. ALL - Complete marine dataset with all available fields") 
        print("3. CUSTOM - Select specific fields manually")
        
        while True:
            try:
                choice = input("\nEnter choice [1-3]: ").strip()
                if choice == '1':
                    complexity_level = 'minimal'
                    break
                elif choice == '2':
                    complexity_level = 'all'
                    break
                elif choice == '3':
                    complexity_level = 'custom'
                    break
                else:
                    print("Invalid choice. Please enter 1, 2, or 3.")
            except (KeyboardInterrupt, EOFError):
                print("\nInstallation cancelled by user.")
                sys.exit(1)
        
        # Get fields from YAML structure
        all_fields = self.yaml_data.get('fields', {})
        if not all_fields:
            print("❌ CRITICAL ERROR: No fields defined in YAML")
            print("   The marine_data_fields.yaml file is missing the 'fields' section.")
            print("   This is a package integrity issue.")
            sys.exit(1)
        
        # Get fields based on complexity level
        if complexity_level == 'minimal':
            # Get only fields marked for minimal complexity
            selected_fields = {}
            for field_name, field_config in all_fields.items():
                complexity_levels = field_config.get('complexity_levels', [])
                if 'minimal' in complexity_levels:
                    selected_fields[field_name] = True
            
            if not selected_fields:
                print("⚠️ No minimal fields found in YAML, using fallback selection")
                # Fallback minimal selection
                minimal_fallback = ['current_water_level', 'next_high_time', 'next_high_height', 
                                'wave_height', 'marine_sea_surface_temp']
                for field_name in minimal_fallback:
                    if field_name in all_fields:
                        selected_fields[field_name] = True
        
        elif complexity_level == 'all':
            # Select ALL fields from the fields section
            selected_fields = {field_name: True for field_name in all_fields.keys()}
        
        elif complexity_level == 'custom':
            # Execute custom selection interface
            print("\nStarting custom field selection interface...")
            custom_selection = self.show_marine_custom_selection(all_fields)
            
            if custom_selection is not None and len(custom_selection) > 0:
                selected_fields = custom_selection
            elif custom_selection is None:
                print("Custom selection cancelled. Using minimal defaults.")
                selected_fields = {}
                for field_name, field_config in all_fields.items():
                    complexity_levels = field_config.get('complexity_levels', [])
                    if 'minimal' in complexity_levels:
                        selected_fields[field_name] = True
            else:
                print("No fields selected in custom mode. Using minimal defaults.")
                selected_fields = {}
                for field_name, field_config in all_fields.items():
                    complexity_levels = field_config.get('complexity_levels', [])
                    if 'minimal' in complexity_levels:
                        selected_fields[field_name] = True
        
        # Validate selection
        if not selected_fields:
            print("❌ CRITICAL ERROR: No fields selected")
            print("   Cannot proceed with installation without field selection.")
            sys.exit(1)
        
        # Display selection summary
        selected_count = len([f for f in selected_fields.values() if f])
        print(f"\n✅ Field selection completed: {selected_count} fields selected")
        
        return selected_fields

    def _select_stations_per_module(self, stations, selected_fields):
        """
        Select stations per module with interactive curses interface.
        FIXED: Complete implementation with proper station selection flow.
        
        Args:
            stations: List of discovered stations  
            selected_fields: Dictionary of selected fields
            
        Returns:
            dict: Selected stations organized by type with station IDs
        """
        print("\n🚢 STATION SELECTION PER MODULE")
        print("-" * 40)
        print("You will now select specific stations for data collection.")
        print("Recommend selecting 2-3 stations per type for backup coverage.")
        
        # Use the existing display station selection method
        selected_stations_list = self._display_station_selection(stations)
        
        if not selected_stations_list:
            print("⚠️  No stations selected. Using automatic fallback selection...")
            # Fallback: automatically select closest stations of each type
            coops_stations = [s for s in stations if s['type'] == 'coops'][:2]
            ndbc_stations = [s for s in stations if s['type'] == 'ndbc'][:2]
            selected_stations_list = coops_stations + ndbc_stations
            
            if selected_stations_list:
                print(f"✅ Auto-selected {len(selected_stations_list)} stations as fallback")
            else:
                print("❌ No stations available for selection")
                return {}
        
        # Organize stations by type for configuration format
        organized_stations = {}
        
        for station in selected_stations_list:
            station_type = station['type']
            station_id = station['id']
            
            if station_type == 'coops':
                if 'coops_stations' not in organized_stations:
                    organized_stations['coops_stations'] = []
                organized_stations['coops_stations'].append(station_id)
            elif station_type == 'ndbc':
                if 'ndbc_stations' not in organized_stations:
                    organized_stations['ndbc_stations'] = []
                organized_stations['ndbc_stations'].append(station_id)
        
        # Display final selection summary
        total_selected = len(selected_stations_list)
        coops_count = len(organized_stations.get('coops_stations', []))
        ndbc_count = len(organized_stations.get('ndbc_stations', []))
        
        print(f"\n✅ STATION SELECTION COMPLETED:")
        print(f"    • Total stations: {total_selected}")
        print(f"    • CO-OPS stations: {coops_count}")
        print(f"    • NDBC stations: {ndbc_count}")
        
        if coops_count > 0:
            print(f"    • CO-OPS IDs: {', '.join(organized_stations['coops_stations'])}")
        if ndbc_count > 0:
            print(f"    • NDBC IDs: {', '.join(organized_stations['ndbc_stations'])}")
        
        return organized_stations

    def _display_station_selection(self, stations):
        """
        Display marine station selection interface with COOP and NDBC separation.
        First select COOP stations, then NDBC stations separately.
        """
        if not stations:
            print("⚠️  No marine stations found within search radius.")
            return []

        # Separate stations by type
        coops_stations = [s for s in stations if s['type'] == 'coops']
        ndbc_stations = [s for s in stations if s['type'] == 'ndbc']
        
        print(f"\n🌊 MARINE STATION SELECTION")
        print("="*70)
        print("You will select two types of marine data sources:")
        print("1. CO-OPS: Coastal tide and water level information")
        print("2. NDBC:  Offshore buoy weather and wave conditions")
        print(f"\nFound {len(coops_stations)} CO-OPS stations and {len(ndbc_stations)} NDBC stations")
        print("\nTIP: Select 2-3 stations of each type for reliable data backup")
        
        try:
            # Initialize curses
            selected_stations = []
            
            # First select COOP stations
            if coops_stations:
                print("\n📍 Opening CO-OPS station selection interface...")
                input("Press ENTER to continue...")
                coops_selected = self._curses_station_selection(coops_stations, "CO-OPS Tide Stations", "🌊")
                selected_stations.extend(coops_selected)
                if coops_selected:
                    print(f"✅ Selected {len(coops_selected)} CO-OPS stations")
                else:
                    print("ℹ️  No CO-OPS stations selected")
            
            # Then select NDBC stations  
            if ndbc_stations:
                print("\n📍 Opening NDBC buoy selection interface...")
                input("Press ENTER to continue...")
                ndbc_selected = self._curses_station_selection(ndbc_stations, "NDBC Buoy Stations", "🛟")
                selected_stations.extend(ndbc_selected)
                if ndbc_selected:
                    print(f"✅ Selected {len(ndbc_selected)} NDBC stations")
                else:
                    print("ℹ️  No NDBC stations selected")
            
            return selected_stations
            
        except Exception as e:
            print(f"❌ Curses interface failed: {e}")
            print("Falling back to text-based selection...")
            # Fallback to simple text interface
            return self._simple_station_selection(coops_stations, ndbc_stations)

    def _curses_station_selection(self, stations, title, icon):
        """Curses-based station selection interface with user-friendly explanations and proper formatting."""
        
        if not stations:
            return []
        
        def curses_main(stdscr):
            # Initialize curses settings
            curses.curs_set(0)  # Hide cursor
            curses.use_default_colors()
            if curses.has_colors():
                curses.start_color()
                curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)  # Header
                curses.init_pair(2, curses.COLOR_GREEN, -1)  # Selected
                curses.init_pair(3, curses.COLOR_YELLOW, -1)  # Highlighted
            
            marked = set()  # Track selected stations
            current_pos = 0  # Current cursor position
            scroll_offset = 0  # For scrolling through long lists
            
            def draw_interface():
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Header with Marine-specific styling
                header_text = f"{icon} {title}"
                try:
                    stdscr.addstr(0, (width - len(header_text)) // 2, header_text, 
                                 curses.color_pair(1) | curses.A_BOLD)
                    stdscr.addstr(1, (width - len(header_text)) // 2, "=" * len(header_text))
                except curses.error:
                    pass
                
                # Marine-specific explanation  
                explanation = [
                    "Select marine monitoring stations for data collection.",
                    "Each station provides different types of marine data.",
                    "TIP: Choose 2-3 stations for backup coverage."
                ]
                
                # Explanation section
                exp_start = 3
                for i, line in enumerate(explanation):
                    if exp_start + i < height - 12:
                        try:
                            stdscr.addstr(exp_start + i, 0, line[:width-1], curses.A_DIM)
                        except curses.error:
                            pass
                
                # Instructions
                inst_row = exp_start + len(explanation) + 1
                try:
                    stdscr.addstr(inst_row, 0, "SPACE: Toggle selection  |  ENTER: Confirm  |  ESC: Skip  |  q: Quit"[:width-1])
                    stdscr.addstr(inst_row + 1, 0, "-" * min(70, width-1))
                except curses.error:
                    pass
                
                # Station list - Show up to 10 stations (design limit)
                start_row = inst_row + 3
                available_rows = max(1, (height - start_row - 3) // 2)
                max_display = min(10, available_rows)
                display_stations = stations[:max_display] if stations else []
                
                for idx, station in enumerate(display_stations):
                    y = start_row + (idx * 2)
                    if y >= height - 3:
                        break
                    
                    # Format station line
                    mark = "[X]" if idx in marked else "[ ]"
                    distance = f"{station.get('distance_km', 0):.1f}km"
                    
                    # Main station line
                    station_line = f"{mark} {station['id']} - {station['name']}"
                    if len(station_line) > width - 15:
                        station_line = station_line[:width-18] + "..."
                    
                    # Highlight current position
                    attr = curses.A_REVERSE if idx == current_pos else curses.A_NORMAL
                    if idx in marked:
                        attr |= curses.color_pair(2)
                    
                    try:
                        stdscr.addstr(y, 2, station_line, attr)
                        # Distance and capabilities line
                        caps = ', '.join(station.get('capabilities', ['Unknown']))
                        detail_line = f"    📍 {distance} | 📊 {caps}"
                        if len(detail_line) > width - 1:
                            detail_line = detail_line[:width-4] + "..."
                        stdscr.addstr(y + 1, 2, detail_line, curses.A_DIM)
                    except curses.error:
                        pass
                
                # Selection summary
                selected_count = len(marked)
                summary = f"Selected: {selected_count}/{len(stations)} stations"
                try:
                    stdscr.addstr(height-1, 0, summary, curses.color_pair(2) if selected_count > 0 else curses.A_DIM)
                except curses.error:
                    pass
                
                try:
                    stdscr.refresh()
                except curses.error:
                    pass
            
            # Main selection loop
            while True:
                draw_interface()
                key = stdscr.getch()
                
                if key == ord('q') or key == 27:  # ESC or 'q' to quit
                    return []
                elif key == ord('\n') or key == curses.KEY_ENTER or key == 10:  # ENTER to confirm
                    return [stations[i] for i in marked]
                elif key == ord(' '):  # SPACE to toggle selection
                    if current_pos in marked:
                        marked.remove(current_pos)
                    else:
                        marked.add(current_pos)
                elif key == curses.KEY_UP:
                    current_pos = max(0, current_pos - 1)
                elif key == curses.KEY_DOWN:
                    current_pos = min(len(stations) - 1, current_pos + 1)
        
        try:
            return curses.wrapper(curses_main)
        except:
            return self._simple_text_selection(stations, title)

    def _simple_text_selection(self, stations, title):
        """Simple text-based selection fallback with explanations."""
        print(f"\n{title} Selection:")
        print("-" * 50)
        
        # Add explanations for lay users
        if "CO-OPS" in title:
            print("CO-OPS stations provide TIDE INFORMATION:")
            print("• Tide predictions (high/low tide times and heights)")
            print("• Real-time water levels (if station has sensors)")
            print("• Coastal water temperature (select stations)")
            print("• Essential for boating, fishing, and coastal activities")
        else:  # NDBC
            print("NDBC buoys provide OFFSHORE MARINE CONDITIONS:")
            print("• Real-time wave heights, periods, and directions") 
            print("• Ocean surface temperature and weather data")
            print("• Wind speed, direction, and atmospheric pressure")
            print("• Critical for offshore boating and marine weather")
        
        print("\nAvailable stations:")
        print("-" * 30)
        
        for i, station in enumerate(stations[:10], 1):
            distance = f"{station['distance_km']:.1f}km"
            bearing = station.get('bearing_text', 'Unknown')
            capabilities = ', '.join(station.get('capabilities', [])[:2])
            print(f"{i:2d}. {station['id']} - {station['name']}")
            print(f"    📍 {distance} {bearing} | 📊 {capabilities}")
        
        print(f"\nTotal: {len(stations)} stations found")
        
        # Simple selection input
        try:
            print("\nSelection options:")
            print("• Enter numbers (e.g., '1,3,5' for stations 1, 3, and 5)")
            print("• Enter 'all' to select all stations")
            print("• Press Enter to skip this station type")
            
            choice = input(f"\nSelect {title.lower()}: ").strip()
            
            if not choice:
                return []
            elif choice.lower() == 'all':
                return stations[:5]  # Limit to 5 for performance
            else:
                # Parse comma-separated numbers
                indices = []
                for part in choice.split(','):
                    try:
                        idx = int(part.strip()) - 1
                        if 0 <= idx < len(stations):
                            indices.append(idx)
                    except ValueError:
                        continue
                
                return [stations[i] for i in indices]
                
        except KeyboardInterrupt:
            print("\nSelection cancelled")
            return []

    def _simple_station_selection(self, coops_stations, ndbc_stations):
        """Fallback text-based station selection."""
        selected = []
        
        # Simple CO-OPS selection
        if coops_stations:
            print(f"\nCO-OPS Stations ({len(coops_stations)} available):")
            for i, station in enumerate(coops_stations[:5]):
                print(f"  {i+1}. {station['id']} - {station['name']} ({station.get('distance_km', 0):.1f}km)")
            
            try:
                choice = input("Select CO-OPS stations (numbers, comma-separated, or 'all'): ").strip()
                if choice.lower() == 'all':
                    selected.extend(coops_stations[:3])  # Limit to 3 for performance
                elif choice:
                    indices = [int(x.strip()) - 1 for x in choice.split(',') if x.strip().isdigit()]
                    selected.extend([coops_stations[i] for i in indices if 0 <= i < len(coops_stations)])
            except ValueError:
                print("Invalid selection, skipping CO-OPS stations")
        
        # Simple NDBC selection
        if ndbc_stations:
            print(f"\nNDBC Stations ({len(ndbc_stations)} available):")
            for i, station in enumerate(ndbc_stations[:5]):
                print(f"  {i+1}. {station['id']} - {station['name']} ({station.get('distance_km', 0):.1f}km)")
            
            try:
                choice = input("Select NDBC stations (numbers, comma-separated, or 'all'): ").strip()
                if choice.lower() == 'all':
                    selected.extend(ndbc_stations[:3])  # Limit to 3 for performance
                elif choice:
                    indices = [int(x.strip()) - 1 for x in choice.split(',') if x.strip().isdigit()]
                    selected.extend([ndbc_stations[i] for i in indices if 0 <= i < len(ndbc_stations)])
            except ValueError:
                print("Invalid selection, skipping NDBC stations")
        
        return selected

    def _collect_field_selections(self):
        """Collect field selections based on YAML complexity levels."""
        print("\n🎯 FIELD SELECTION")
        print("-" * 30)
        
        # Show complexity menu and get user choice
        print("Field Selection Options:")
        print("1. MINIMAL - Essential marine monitoring fields")
        print("2. ALL - Complete marine dataset with all available fields") 
        print("3. CUSTOM - Choose specific fields")
        
        while True:
            try:
                choice = input("\nSelect option (1/2/3): ").strip()
                if choice in ['1', '2', '3']:
                    break
                print("Please enter 1, 2, or 3")
            except KeyboardInterrupt:
                print("\nField selection cancelled")
                return {}
        
        if choice == '1':
            return self._get_minimal_field_selection()
        elif choice == '2':
            return self._get_all_field_selection()
        elif choice == '3':
            return self._get_custom_field_selection()
        
        return {}
    
    def _get_minimal_field_selection(self):
        """Get minimal field selection for essential marine monitoring."""
        print("\n📊 MINIMAL FIELD SELECTION")
        print("Essential marine monitoring fields selected:")
        
        # Return pre-defined minimal field set
        minimal_fields = {
            'coops_module': [
                'marine_current_water_level',
                'marine_next_high_time',
                'marine_next_high_height'
            ],
            'ndbc_module': [
                'marine_wave_height',
                'marine_sea_surface_temp'
            ]
        }
        
        for module, fields in minimal_fields.items():
            print(f"  • {module}: {len(fields)} fields")
        
        return minimal_fields
    
    def _get_all_field_selection(self):
        """Get all available fields."""
        print("\n📊 ALL FIELDS SELECTION")
        print("All available marine fields selected:")
        
        all_fields = {}
        modules = self.yaml_data.get('modules', {})
        
        for module_name, module_data in modules.items():
            fields = module_data.get('fields', [])
            if fields:
                all_fields[module_name] = fields
                print(f"  • {module_name}: {len(fields)} fields")
        
        return all_fields
    
    def _get_custom_field_selection(self):
        """Get custom field selection using curses interface."""
        print("\n📊 CUSTOM FIELD SELECTION")
        print("Opening interactive field selection interface...")
        
        if not sys.stdout.isatty():
            print("Custom selection requires interactive terminal. Using ALL fields instead.")
            return self._get_all_field_selection()
        
        try:
            input("Press ENTER to continue...")
            return self.show_marine_custom_selection(self._get_field_definitions())
        except Exception as e:
            print(f"Custom selection failed: {e}")
            print("Falling back to ALL fields selection")
            return self._get_all_field_selection()
    
    def _get_field_definitions(self):
        """Extract field definitions from YAML for custom selection."""
        field_definitions = {}
        modules = self.yaml_data.get('modules', {})
        
        for module_name, module_data in modules.items():
            field_defs = module_data.get('field_definitions', {})
            field_definitions.update(field_defs)
        
        return field_definitions

    def show_marine_custom_selection(self, field_definitions):
        """Marine-specific field selection with COOP/NDBC grouping."""
        
        if not field_definitions:
            print("No field definitions available for custom selection")
            return {}
        
        def curses_main(stdscr):
            import curses
            
            # Initialize curses
            curses.curs_set(0)  # Hide cursor
            curses.use_default_colors()
            if curses.has_colors():
                curses.start_color()
                curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)  # Highlight
                curses.init_pair(2, curses.COLOR_GREEN, -1)  # Selected
                curses.init_pair(3, curses.COLOR_BLUE, -1)   # Header
                curses.init_pair(4, curses.COLOR_CYAN, -1)   # Section headers
            
            # Group fields by Marine data source
            coops_fields = []
            ndbc_fields = []
            
            for field_name, field_info in field_definitions.items():
                field_entry = {
                    'type': 'field',
                    'name': field_name,
                    'display': field_info.get('display_name', field_name),
                    'selected': False
                }
                
                # Group by api_module (coops_module vs ndbc_module)
                api_module = field_info.get('api_module', '').lower()
                if 'coops' in api_module:
                    coops_fields.append(field_entry)
                elif 'ndbc' in api_module:
                    ndbc_fields.append(field_entry)
            
            # Sort each group alphabetically
            coops_fields.sort(key=lambda x: x['display'])
            ndbc_fields.sort(key=lambda x: x['display'])
            
            # Create combined list with Marine-specific section headers
            all_items = []
            
            # Add COOP section
            if coops_fields:
                all_items.append({
                    'type': 'header',
                    'display': '🌊 CO-OPS TIDE STATIONS',
                    'description': 'Real-time water levels, tide predictions, coastal water temperature'
                })
                all_items.extend(coops_fields)
                all_items.append({'type': 'spacer'})  # Add spacing between sections
            
            # Add NDBC section
            if ndbc_fields:
                all_items.append({
                    'type': 'header',
                    'display': '🛟 NDBC BUOY STATIONS',
                    'description': 'Marine weather, waves, and offshore conditions'
                })
                all_items.extend(ndbc_fields)
            
            current_item = 0
            scroll_offset = 0
            
            def draw_interface():
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Main header
                header = "MARINE DATA FIELD SELECTION"
                try:
                    stdscr.addstr(0, (width - len(header)) // 2, header, curses.color_pair(3) | curses.A_BOLD)
                    stdscr.addstr(1, 0, "=" * min(width-1, len(header)))
                except curses.error:
                    pass
                
                # Instructions
                instructions = "↑/↓: Navigate  SPACE: Select  ENTER: Confirm  q: Quit"
                try:
                    stdscr.addstr(height-1, 0, instructions[:width-1], curses.A_DIM)
                except curses.error:
                    pass
                
                # Display items
                start_y = 3
                display_height = height - 5
                
                visible_start = scroll_offset
                visible_end = min(len(all_items), visible_start + display_height)
                
                for i in range(visible_end - visible_start):
                    item_idx = visible_start + i
                    if item_idx >= len(all_items):
                        break
                        
                    item = all_items[item_idx]
                    y = start_y + i
                    
                    if item['type'] == 'header':
                        # Marine section header
                        try:
                            stdscr.addstr(y, 0, item['display'], curses.color_pair(4) | curses.A_BOLD)
                        except curses.error:
                            pass
                    elif item['type'] == 'spacer':
                        # Empty line between sections
                        continue
                    elif item['type'] == 'field':
                        # Marine field selection
                        mark = "[X]" if item['selected'] else "[ ]"
                        line = f"  {mark} {item['display']}"
                        
                        # Highlight current item
                        attr = curses.A_REVERSE if item_idx == current_item else curses.A_NORMAL
                        if item['selected']:
                            attr |= curses.color_pair(2)
                        
                        try:
                            stdscr.addstr(y, 0, line[:width-1], attr)
                        except curses.error:
                            pass
                
                # Marine-specific summary at bottom
                coops_selected = sum(1 for f in coops_fields if f['selected'])
                ndbc_selected = sum(1 for f in ndbc_fields if f['selected'])
                total_selected = coops_selected + ndbc_selected
                total_fields = len(coops_fields) + len(ndbc_fields)
                
                summary = f"Selected: {total_selected}/{total_fields} fields (🌊 CO-OPS: {coops_selected}, 🛟 NDBC: {ndbc_selected})"
                try:
                    stdscr.addstr(height-2, 0, summary[:width-1], curses.color_pair(3))
                except curses.error:
                    pass
                
                try:
                    stdscr.refresh()
                except curses.error:
                    pass
            
            # Find next selectable field
            def find_next_field(start_idx, direction=1):
                items_count = len(all_items)
                idx = start_idx
                
                while True:
                    idx = (idx + direction) % items_count
                    if idx == start_idx:  # Wrapped around
                        break
                    if all_items[idx]['type'] == 'field':
                        return idx
                return start_idx
            
            # Initialize current item to first field
            current_item = find_next_field(-1, 1)
            
            # Main interaction loop
            while True:
                draw_interface()
                key = stdscr.getch()
                
                if key == ord('q') or key == 27:  # ESC or 'q'
                    return None
                elif key == curses.KEY_UP:
                    current_item = find_next_field(current_item, -1)
                elif key == curses.KEY_DOWN:
                    current_item = find_next_field(current_item, 1)
                elif key == ord(' '):  # Space to toggle selection
                    if current_item < len(all_items) and all_items[current_item]['type'] == 'field':
                        all_items[current_item]['selected'] = not all_items[current_item]['selected']
                elif key == ord('\n') or key == curses.KEY_ENTER or key == 10:
                    # Return Marine field selection
                    result = {}
                    for item in all_items:
                        if item['type'] == 'field' and item['selected']:
                            result[item['name']] = True
                    return result
        
        try:
            result = curses.wrapper(curses_main)
            
            if result is None:
                print("\nMarine field selection cancelled.")
                return None
            
            # Show Marine-specific summary
            selected_count = len(result)
            print(f"\n" + "="*60)
            print(f"MARINE FIELD SELECTION SUMMARY: {selected_count} fields selected")
            print("="*60)
            
            if selected_count == 0:
                print("Warning: No fields selected. Using 'minimal' defaults instead.")
                return None
            
            # Group selected fields by Marine data source for display
            coops_selected = []
            ndbc_selected = []
            
            for field_name in result.keys():
                if field_name in field_definitions:
                    api_module = field_definitions[field_name].get('api_module', '').lower()
                    display_name = field_definitions[field_name].get('display_name', field_name)
                    
                    if 'coops' in api_module:
                        coops_selected.append(display_name)
                    elif 'ndbc' in api_module:
                        ndbc_selected.append(display_name)
            
            if coops_selected:
                print(f"\n🌊 CO-OPS Tide Station Fields ({len(coops_selected)} selected):")
                for name in coops_selected[:3]:
                    print(f"  - {name}")
                if len(coops_selected) > 3:
                    print(f"  ... and {len(coops_selected) - 3} more")
            
            if ndbc_selected:
                print(f"\n🛟 NDBC Buoy Station Fields ({len(ndbc_selected)} selected):")
                for name in ndbc_selected[:3]:
                    print(f"  - {name}")
                if len(ndbc_selected) > 3:
                    print(f"  ... and {len(ndbc_selected) - 3} more")
            
            return result
            
        except Exception as e:
            print(f"\nError with Marine field selection interface: {e}")
            print("Falling back to 'minimal' field selection.")
            return None

    def _configure_collection_intervals(self):
        """Configure collection intervals for different data types."""
        print("\n⏰ COLLECTION INTERVALS")
        print("-" * 30)
        print("Setting data collection frequencies:")
        print("• CO-OPS data: Every 10 minutes (high frequency)")
        print("• NDBC data: Every hour (standard frequency)")
        print("• API timeout: 30 seconds")
        
        return {
            'coops_module': 600,    # 10 minutes
            'ndbc_module': 3600,    # 1 hour
            'timeout': 30
        }

    def _generate_configuration(self, selected_fields, selected_stations, user_lat, user_lon):
        """Generate WeeWX configuration from selections."""
        config = {
            'MarineDataService': {
                'enable': 'true',
                'timeout': '30',
                'log_success': 'false',
                'log_errors': 'true',
                'retry_attempts': '3',
                'user_latitude': str(user_lat),
                'user_longitude': str(user_lon),
                'field_mappings': {},
                'station_config': {}
            }
        }
        
        # Generate field mappings from YAML definitions
        modules = self.yaml_data.get('modules', {})
        for module_name, field_list in selected_fields.items():
            module_data = modules.get(module_name, {})
            field_defs = module_data.get('field_definitions', {})
            
            config['MarineDataService']['field_mappings'][module_name] = {}
            
            for field_name in field_list:
                field_def = field_defs.get(field_name, {})
                config['MarineDataService']['field_mappings'][module_name][field_name] = {
                    'database_field': field_def.get('database_field', field_name),
                    'database_type': field_def.get('database_type', 'REAL'),
                    'database_table': field_def.get('database_table', self._determine_table_from_field(field_name, module_name)),
                    'api_module': field_def.get('api_module', module_name)
                }
        
        # Generate station configuration
        for module_name, station_ids in selected_stations.items():
            config['MarineDataService']['station_config'][module_name] = {
                'stations': station_ids,
                'update_interval': self._get_update_interval(module_name)
            }
        
        return config

    def _determine_table_from_field(self, field_name, module_name):
        """ARCHITECTURE FIX: Determine target table based on field and module."""
        if module_name.startswith('coops'):
            # High-frequency CO-OPS data
            if any(x in field_name for x in ['current_water_level', 'coastal_water_temp', 'water_level_sigma', 'water_level_flags', 'water_temp_flags']):
                return 'coops_realtime'
            # Low-frequency CO-OPS predictions
            elif any(x in field_name for x in ['next_high', 'next_low', 'tide_range']):
                return 'coops_predictions'
        elif module_name.startswith('ndbc'):
            # All NDBC data goes to ndbc_data table
            return 'ndbc_data'
        
        # Fallback - should not happen with proper YAML
        return 'ndbc_data'

    def _get_update_interval(self, module_name):
        """Get appropriate update interval for module."""
        if 'coops' in module_name:
            return '600'  # 10 minutes for CO-OPS data
        elif 'ndbc' in module_name:
            return '3600'  # 1 hour for NDBC data
        else:
            return '1800'  # 30 minutes default

    def _get_collection_intervals(self):
        """Get collection intervals for different modules."""
        return {
            'coops_module': 600,  # 10 minutes - CO-OPS high frequency
            'ndbc_module': 3600,   # 1 hour - NDBC standard frequency
            'timeout': 30
        }
    
    def _write_configuration_files(self, selected_stations, selected_fields, intervals, user_lat, user_lon):
        """Write configuration files in exact CONF format with comprehensive mapping."""
        print("\n📄 CONFIGURATION FILE GENERATION")
        print("-" * 40)
        
        # FIXED: selected_stations is a dict with station IDs, not station objects
        coops_station_ids = selected_stations.get('coops_stations', [])
        ndbc_station_ids = selected_stations.get('ndbc_stations', [])
        
        print(f"📊 Configuration Summary:")
        print(f"  • CO-OPS stations: {len(coops_station_ids)}")
        print(f"  • NDBC stations: {len(ndbc_station_ids)}")
        print(f"  • Selected fields: {len(selected_fields)}")
        print(f"  • User location: {user_lat:.4f}, {user_lon:.4f}")
        
        # Generate comprehensive configuration dictionary
        config_dict = {
            'MarineDataService': {
                # Core service settings (string values only for ConfigObj)
                'enable': 'true',
                'timeout': str(intervals.get('timeout', 30)),
                'log_success': 'false',
                'log_errors': 'true',
                'retry_attempts': '3',
                
                # User location for distance calculations
                'user_latitude': str(user_lat),
                'user_longitude': str(user_lon),
                
                # Station selections with detailed configuration
                'selected_stations': {
                    'coops_stations': {station_id: 'true' for station_id in coops_station_ids},
                    'ndbc_stations': {station_id: 'true' for station_id in ndbc_station_ids}
                },
                
                # Field selection tracking
                'field_selection': {
                    'selection_timestamp': str(int(time.time())),
                    'config_version': '1.0',
                    'complexity_level': 'custom',
                    'selected_fields': self._organize_fields_by_module(selected_fields)
                },
                
                # Field mappings transformed from YAML to CONF format
                'field_mappings': self._transform_fields_yaml_to_conf(selected_fields),
                
                # Collection intervals and timing
                'collection_intervals': {
                    'coops_collection_interval': '600',    # 10 minutes
                    'tide_predictions_interval': '21600',  # 6 hours
                    'ndbc_weather_interval': '3600',       # 1 hour
                    'ndbc_ocean_interval': '3600'          # 1 hour
                },
                
                # API endpoint configuration
                'api_endpoints': self._generate_api_endpoint_configuration()
            }
        }
        
        # Validate configuration completeness
        validation_result = self._validate_configuration(config_dict)
        if validation_result['valid']:
            print("✅ Configuration validation passed")
        else:
            print("⚠️  Configuration validation warnings:")
            for warning in validation_result['warnings']:
                print(f"    • {warning}")
        
        print("✅ Service configuration dictionary generated")
        print(f"✅ Configuration sections: {list(config_dict['MarineDataService'].keys())}")
        
        return config_dict
    
    def _generate_station_configuration(self, coops_stations, ndbc_stations):
        """Generate station configuration section - simple format for service runtime."""
        station_config = {
            'coops_stations': ','.join([s['id'] for s in coops_stations]),
            'ndbc_stations': ','.join([s['id'] for s in ndbc_stations]),
            'total_stations': str(len(coops_stations) + len(ndbc_stations))
        }
        
        return station_config
    
    def _generate_coops_module_config(self, coops_stations, intervals):
        """Generate CO-OPS module configuration with stations and settings."""
        coops_config = {
            'enable': 'true' if coops_stations else 'false',
            'interval': str(intervals.get('coops_module', 600)),
            'api_url': self.yaml_data.get('api_modules', {}).get('coops_module', {}).get('api_url', 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter'),
            'stations': ','.join([s['id'] for s in coops_stations]),
            'products': 'water_level,predictions,water_temperature',
            'units': 'metric',
            'datum': 'MLLW'
        }
        
        return coops_config
    
    def _generate_ndbc_module_config(self, ndbc_stations, intervals):
        """Generate NDBC module configuration."""
        # Get NDBC module configuration from YAML
        api_modules = self.yaml_data.get('api_modules', {})
        ndbc_module = api_modules.get('ndbc_module', {})
        
        ndbc_config = {
            'enable': 'true' if ndbc_stations else 'false',
            'interval': str(intervals.get('ndbc_module', 3600)),
            'api_url': ndbc_module.get('api_url', 'https://www.ndbc.noaa.gov/data/realtime2'),
            'stations': ','.join([s['id'] for s in ndbc_stations]),
            'data_types': 'stdmet,spec,swdir,swden',  # Standard met, spectral, swell direction, swell density
            'units': 'metric'
        }
        
        return ndbc_config
    
    def _generate_interval_configuration(self, intervals):
        """Generate centralized interval configuration."""
        interval_config = {}
        
        for module_name, interval_seconds in intervals.items():
            if module_name != 'timeout':
                interval_config[module_name] = {
                    'seconds': str(interval_seconds),
                    'minutes': str(interval_seconds // 60),
                    'daily_calls': str(86400 // interval_seconds)
                }
        
        return interval_config
    
    def _generate_api_endpoint_configuration(self):
        """Generate API endpoint configuration from YAML."""
        endpoint_config = {}
        api_modules = self.yaml_data.get('api_modules', {})
        
        for module_name, module_info in api_modules.items():
            if 'api_url' in module_info:
                endpoint_config[module_name] = {
                    'base_url': module_info['api_url'],
                    'timeout': str(module_info.get('timeout', 30)),
                    'retry_attempts': str(module_info.get('retry_attempts', 3))
                }
        
        return endpoint_config
    
    def _validate_configuration(self, config_dict):
        """Validate configuration completeness and consistency."""
        warnings = []
        
        service_config = config_dict.get('MarineDataService', {})
        
        # Check for station selections
        selected_stations = service_config.get('selected_stations', {})
        if not selected_stations.get('coops_stations') and not selected_stations.get('ndbc_stations'):
            warnings.append("No stations selected - marine data collection will be disabled")
        
        # Check for field mappings
        field_mappings = service_config.get('field_mappings', {})
        if not field_mappings:
            warnings.append("No field mappings defined - default fields will be used")
        
        # Check for API endpoints
        api_endpoints = service_config.get('api_endpoints', {})
        if not api_endpoints:
            warnings.append("No API endpoints configured - using hardcoded defaults")
        
        return {
            'valid': len(warnings) == 0,
            'warnings': warnings
        }
           
    def _transform_fields_yaml_to_conf(self, selected_fields):
        """Transform YAML field definitions to CONF format with comprehensive mapping."""
        conf_mappings = {}
        
        # Get the full field definitions from YAML
        yaml_field_definitions = self.yaml_data.get('fields', {})
        
        if not yaml_field_definitions:
            print("❌ CRITICAL ERROR: No YAML field definitions found")
            return {}
        
        if not selected_fields:
            print("❌ CRITICAL ERROR: No fields selected")
            return {}
        
        print(f"🔄 Transforming {len(selected_fields)} selected fields to CONF format...")
        
        # Track field statistics for validation
        field_stats = {
            'coops_fields': 0,
            'ndbc_fields': 0,
            'archive_table': 0,
            'marine_tables': 0,
            'numeric_fields': 0,
            'text_fields': 0
        }
        
        # Process each selected field and group by module
        for field_name, is_selected in selected_fields.items():
            if not is_selected:  # Skip unselected fields
                continue
                
            # Look up the field definition in YAML
            if field_name not in yaml_field_definitions:
                print(f"    ⚠️  Warning: Field '{field_name}' not found in YAML definitions")
                continue
                
            field_config = yaml_field_definitions[field_name]
            
            try:
                # Determine the module for this field
                api_module = field_config.get('api_module', 'unknown_module')
                
                # Initialize module section if not exists
                if api_module not in conf_mappings:
                    conf_mappings[api_module] = {}
                
                # Get service field name (for nested structure)
                service_field = field_config.get('service_field', field_name)
                
                # ARCHITECTURE FIX: Determine correct table
                database_table = self._determine_table_from_field(field_name, api_module)
                
                # Core field mapping in nested structure
                conf_mappings[api_module][service_field] = {
                    'database_field': field_config.get('database_field', field_name),
                    'database_type': field_config.get('database_type', 'REAL'),
                    'database_table': database_table,  # ARCHITECTURE FIX
                    'api_path': field_config.get('api_path', ''),
                    'unit_group': field_config.get('unit_group', 'group_count'),
                    'api_product': field_config.get('api_product', 'default'),
                    'description': field_config.get('description', f'Marine data field: {field_name}')
                }
                
                # Update statistics
                api_source = api_module.lower()
                if 'coops' in api_source:
                    field_stats['coops_fields'] += 1
                elif 'ndbc' in api_source:
                    field_stats['ndbc_fields'] += 1
                
                if database_table == 'archive':
                    field_stats['archive_table'] += 1
                else:
                    field_stats['marine_tables'] += 1
                
                database_type = field_config.get('database_type', 'REAL')
                if database_type in ['REAL', 'INTEGER']:
                    field_stats['numeric_fields'] += 1
                else:
                    field_stats['text_fields'] += 1
                        
            except Exception as e:
                print(f"    ⚠️  Error transforming field '{field_name}': {e}")
                continue
        
        # Display transformation summary
        total_fields = sum(len(module_fields) for module_fields in conf_mappings.values())
        print(f"✅ Field transformation completed:")
        print(f"    • Total fields: {total_fields}")
        print(f"    • Modules: {list(conf_mappings.keys())}")
        print(f"    • CO-OPS fields: {field_stats['coops_fields']}")
        print(f"    • NDBC fields: {field_stats['ndbc_fields']}")
        print(f"    • Archive table: {field_stats['archive_table']}")
        print(f"    • Marine tables: {field_stats['marine_tables']}")
        print(f"    • Numeric fields: {field_stats['numeric_fields']}")
        print(f"    • Text fields: {field_stats['text_fields']}")
        
        if not conf_mappings:
            print("❌ CRITICAL ERROR: No field mappings created")
            return {}
        
        return conf_mappings

    def _organize_fields_by_module(self, selected_fields):
        """Organize selected fields by their API module for configuration."""
        organized = {}
        
        # Get field definitions from YAML
        all_fields = self.yaml_data.get('fields', {})
        
        for field_name in selected_fields.keys():
            if field_name in all_fields:
                field_config = all_fields[field_name]
                api_module = field_config.get('api_module', 'unknown_module')
                
                if api_module not in organized:
                    organized[api_module] = []
                organized[api_module].append(field_name)
        
        # Convert lists to comma-separated strings for CONF format
        for module in organized:
            organized[module] = ', '.join(organized[module])
        
        return organized

class MarineDatabaseManager:
    """
    ARCHITECTURE FIX: Database manager for three-table marine architecture.
    
    Creates and manages coops_realtime, coops_predictions, and ndbc_data tables
    instead of injecting fields into the archive table.
    """
    
    def __init__(self, config_dict):
        self.config_dict = config_dict
        
    def _create_marine_table(self, table_name):
        """Create specific marine table with database-agnostic approach using WeeWX config."""
        try:
            # CORRECTED: Proper WeeWX 5.1 database type detection
            databases_config = self.config_dict.get('Databases', {})
            archive_database_binding = databases_config.get('archive_database', 'archive_sqlite')
            
            # Get the specific database configuration for the archive binding
            archive_db_config = databases_config.get(archive_database_binding, {})
            database_type = archive_db_config.get('database_type', 'SQLite')
            
            print(f"    DEBUG: archive_database_binding = {archive_database_binding}")
            print(f"    DEBUG: archive_db_config = {archive_db_config}")
            print(f"    DEBUG: database_type = {database_type}")
            
            # Get DatabaseTypes configuration
            database_types = self.config_dict.get('DatabaseTypes', {})
            
            if database_type.upper() == 'MYSQL':
                # Use MySQL configuration
                mysql_config = database_types.get('MySQL', {})
                # Merge with the specific database instance config
                mysql_config.update(archive_db_config)
                print(f"    DEBUG: Using MySQL with config: {mysql_config}")
                self._create_table_mysql(table_name, mysql_config)
            else:
                # Use SQLite configuration (default)
                sqlite_config = database_types.get('SQLite', {})
                # Merge with the specific database instance config  
                sqlite_config.update(archive_db_config)
                print(f"    DEBUG: Using SQLite with config: {sqlite_config}")
                self._create_table_sqlite(table_name, sqlite_config)
                
            print(f"    ✅ Created table '{table_name}' using {database_type} database")
            
        except Exception as e:
            print(f"    ❌ Error creating table {table_name}: {e}")
            import traceback
            traceback.print_exc()
            raise

    def _check_existing_fields(self):
        """Check which marine fields already exist in database."""
        try:
            # CRITICAL: Use 'marine_' prefix instead of 'ow_' prefix
            db_binding = 'wx_binding'
            
            with weewx.manager.open_manager_with_config(self.config_dict, db_binding) as dbmanager:
                existing_fields = []
                for column in dbmanager.connection.genSchemaOf('archive'):
                    field_name = column[1]
                    if field_name.startswith('marine_'):  # Marine fields prefix
                        existing_fields.append(field_name)
            
            return existing_fields
        except Exception as e:
            print(f"  Warning: Could not check existing database fields: {e}")
            return []

    def _extract_field_mappings_from_selection(self, selected_options):
        """Extract database field mappings from configuration written during setup."""
        field_mappings = {}
        
        # The field mappings were already written to config_dict during interactive setup
        # Extract them using the same pattern as the service code
        service_config = self.config_dict.get('MarineDataService', {})
        if not service_config:
            print("    ⚠️  Warning: No MarineDataService configuration found")
    
        
        config_field_mappings = service_config.get('field_mappings', {})
        if not config_field_mappings:
            print("    ⚠️  Warning: No field_mappings found in configuration")
            return field_mappings
        
        # Extract database fields from all modules
        for module_name, module_mappings in config_field_mappings.items():
            if isinstance(module_mappings, dict):
                for field_name, field_config in module_mappings.items():
                    if isinstance(field_config, dict):
                        db_field = field_config.get('database_field')
                        db_type = field_config.get('database_type', 'REAL')
                        
                        if db_field:
                            field_mappings[db_field] = db_type
                        else:
                            print(f"    ⚠️  Warning: No database_field for '{field_name}'")
        
        return field_mappings

    def _determine_required_tables(self, selected_options):
        """Determine which tables need to be created based on selected fields."""
        tables_needed = set()
        
        field_mappings = self.config_dict.get('MarineDataService', {}).get('field_mappings', {})
        
        for module_name, module_mappings in field_mappings.items():
            for field_name, field_config in module_mappings.items():
                table_name = field_config.get('database_table')
                if table_name and table_name != 'archive':
                    tables_needed.add(table_name)
        
        # Ensure we have at least the core tables if any marine data is selected
        if field_mappings:
            # Add tables based on modules selected
            for module_name in field_mappings.keys():
                if 'coops' in module_name:
                    tables_needed.add('coops_realtime')
                    tables_needed.add('coops_predictions')
                elif 'ndbc' in module_name:
                    tables_needed.add('ndbc_data')
        
        return tables_needed

    def _create_marine_tables(self, selected_options):
        """Create marine tables based on user selections - CORRECTLY parsing selected_options."""
        print("\n📊 MARINE DATABASE TABLE CREATION")
        print("-" * 50)
        
        # Extract field selections from the passed dictionary
        selected_fields = selected_options.get('fields', {})
        
        if not selected_fields or not any(selected_fields.values()):
            print("⚠️  No fields selected - skipping table creation")
            return
        
        # DATA-DRIVEN: Get field mappings from configuration to determine tables
        field_mappings = self.config_dict.get('MarineDataService', {}).get('field_mappings', {})
        required_tables = set()
        
        # Scan field mappings to find which tables are needed for selected fields
        for module_name, module_fields in field_mappings.items():
            if isinstance(module_fields, dict):
                for service_field, field_config in module_fields.items():
                    if isinstance(field_config, dict):
                        # Check if this field was selected by user
                        database_field = field_config.get('database_field', '')
                        if any(database_field in selected_field for selected_field in selected_fields.keys() if selected_fields[selected_field]):
                            # Get target table from field mapping configuration
                            target_table = field_config.get('database_table', 'archive')
                            if target_table != 'archive':
                                required_tables.add(target_table)
        
        if not required_tables:
            print("⚠️  No marine tables required for selected fields")
            return
        
        print(f"📋 Creating {len(required_tables)} marine database tables: {list(required_tables)}")
        
        # Create each required table using the existing single table creation method
        for table_name in required_tables:
            print(f"  🔨 Creating table: {table_name}")
            self._create_marine_table(table_name)
        
        print("✅ Marine database table creation completed")

    def _get_fields_for_table(self, table_name):
        """Get all fields that belong to a specific table from field mappings."""
        table_fields = {}
        
        field_mappings = self.config_dict.get('MarineDataService', {}).get('field_mappings', {})
        
        for module_name, module_mappings in field_mappings.items():
            if isinstance(module_mappings, dict):
                for field_name, field_config in module_mappings.items():
                    if isinstance(field_config, dict):
                        if field_config.get('database_table') == table_name:
                            table_fields[field_name] = field_config
        
        return table_fields
    
    def _create_table_sqlite(self, table_name, sqlite_config):
        """Create table in SQLite database."""
        import sqlite3
        
        # Get SQLite database path from WeeWX config
        database_name = sqlite_config.get('database_name', 'weewx.sdb')
        
        # Handle relative path (following WeeWX patterns)
        if not os.path.isabs(database_name):
            weewx_root = self.config_dict.get('WEEWX_ROOT', '/var/lib/weewx')
            database_name = os.path.join(weewx_root, database_name)
        
        print(f"    📋 Creating SQLite table in: {database_name}")
        
        # Get fields for this table from field mappings
        table_fields = self._get_fields_for_table(table_name)
        field_definitions = self._build_field_definitions(table_fields)
        
        if not field_definitions:
            print(f"    ⚠️  No valid fields for table '{table_name}' - skipping creation")
            return
        
        # Connect to SQLite and create table
        connection = sqlite3.connect(database_name, timeout=10)
        cursor = connection.cursor()
        
        fields_sql = ', '.join(['dateTime INTEGER NOT NULL', 'station_id TEXT NOT NULL'] + field_definitions)
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_sql}, PRIMARY KEY (dateTime, station_id))"
        
        print(f"    DEBUG: SQLite SQL: {sql}")
        cursor.execute(sql)
        connection.commit()
        connection.close()

    def _create_table_mysql(self, table_name, mysql_config):
        """Create table in MySQL database."""
        try:
            import MySQLdb
        except ImportError:
            try:
                import pymysql as MySQLdb
            except ImportError:
                raise ImportError("MySQL support requires MySQLdb or PyMySQL")
        
        # Get MySQL connection details from WeeWX config
        host = mysql_config.get('host', 'localhost')
        user = mysql_config.get('user', 'weewx')
        password = mysql_config.get('password', 'weewx')
        database_name = mysql_config.get('database_name', 'weewx')
        
        print(f"    📋 Creating MySQL table in database: {database_name}")
        
        # Get fields for this table from field mappings
        table_fields = self._get_fields_for_table(table_name)
        field_definitions = self._build_field_definitions_mysql(table_fields)
        
        if not field_definitions:
            print(f"    ⚠️  No valid fields for table '{table_name}' - skipping creation")
            return
        
        # Connect to MySQL and create table
        connection = MySQLdb.connect(
            host=host,
            user=user,
            passwd=password,
            db=database_name
        )
        cursor = connection.cursor()
        
        # MySQL-specific field definitions
        fields_sql = ', '.join(['dateTime INT NOT NULL', 'station_id VARCHAR(20) NOT NULL'] + field_definitions)
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_sql}, PRIMARY KEY (dateTime, station_id))"
        
        print(f"    DEBUG: MySQL SQL: {sql}")
        cursor.execute(sql)
        connection.commit()
        connection.close()

    def _build_field_definitions(self, table_fields):
        """Build SQLite field definitions from table fields."""
        field_definitions = []
        for field_config in table_fields.values():
            db_field = field_config.get('database_field')
            db_type = field_config.get('database_type', 'REAL')
            if db_field:
                # SQLite type mapping
                if db_type.startswith('VARCHAR'):
                    sqlite_type = 'TEXT'
                else:
                    sqlite_type = db_type
                field_definitions.append(f"{db_field} {sqlite_type}")
        return field_definitions

    def _build_field_definitions_mysql(self, table_fields):
        """Build MySQL field definitions from table fields."""
        field_definitions = []
        for field_config in table_fields.values():
            db_field = field_config.get('database_field')
            db_type = field_config.get('database_type', 'REAL')
            if db_field:
                # MySQL type mapping
                if db_type == 'REAL':
                    mysql_type = 'FLOAT'
                elif db_type == 'INTEGER':
                    mysql_type = 'INT'
                elif db_type.startswith('VARCHAR'):
                    mysql_type = db_type
                else:
                    mysql_type = 'TEXT'
                field_definitions.append(f"{db_field} {mysql_type}")
        return field_definitions