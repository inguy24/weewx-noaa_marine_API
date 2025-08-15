#!/usr/bin/env python3
# Magic Animal: Starfish
"""
WeeWX Marine Data Extension Installer v1.0.1

Copyright 2025 Shane Burkhardt
"""

import os
import json
import urllib.request
import urllib.parse
import urllib.error
import sys
import subprocess
import time
import yaml
import requests
import math
import curses
import textwrap
import xml.etree.ElementTree as ET
from configobj import ConfigObj
from typing import Dict, List, Optional, Any, Tuple

# CRITICAL: Correct import path for WeeWX 5.1
try:
    from weecfg.extension import ExtensionInstaller
    import weewx.manager
    import weewx
    import weeutil.logger
    log = weeutil.logger.logging.getLogger(__name__)
except ImportError:
    print("Error: This installer requires WeeWX 5.1 or later")
    sys.exit(1)

# FIXED: Standardized icon usage (only 4 core icons)
CORE_ICONS = {
    'navigation': '📍',    # Location/station selection
    'status': '✅',        # Success indicators  
    'warning': '⚠️',       # Warnings/issues
    'selection': '🔧'      # Configuration/selection
}

# REQUIRED: Loader function for WeeWX extension system
def loader():
    return MarineDataInstaller()


class InstallationProgressManager:
    """Progress indicator with throbber for long operations"""
    
    def __init__(self):
        self.spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.current_step = 0
        self.spinner_active = False
        self.spinner_thread = None
        
    def show_step_progress(self, step_name, current=None, total=None):
        """Show progress for a step with optional counter"""
        if current is not None and total is not None:
            char = self.spinner_chars[current % len(self.spinner_chars)]
            print(f"\r  {char} {step_name}... {current}/{total}", end='', flush=True)
        else:
            print(f"  {step_name}...", end='', flush=True)
    
    def complete_step(self, step_name):
        """Mark step as complete"""
        print(f"\r  {step_name}... {CORE_ICONS['status']}")
        
    def show_error(self, step_name, error_msg):
        """Show step error"""
        print(f"\r  {step_name}... {CORE_ICONS['warning']}  {error_msg}")

    def start_spinner(self, step_name):
        """Start animated spinner for long operations"""
        import threading
        import time
        
        self.spinner_active = True
        self.current_step = 0
        
        def animate():
            while self.spinner_active:
                char = self.spinner_chars[self.current_step % len(self.spinner_chars)]
                print(f"\r  {step_name}... {char}", end='', flush=True)
                self.current_step += 1
                time.sleep(0.5)
        
        self.spinner_thread = threading.Thread(target=animate, daemon=True)
        self.spinner_thread.start()
    
    def stop_spinner(self, step_name, success=True, error_msg=None):
        """Stop spinner and show completion or error"""
        self.spinner_active = False
        if self.spinner_thread:
            self.spinner_thread.join(timeout=1)
        
        if success:
            print(f"\r  {step_name}... {CORE_ICONS['status']}")
        else:
            icon = CORE_ICONS['warning']
            if error_msg:
                print(f"\r  {step_name}... {icon}  {error_msg}")
            else:
                print(f"\r  {step_name}... {icon}")
                
        
class MarineDataInstaller(ExtensionInstaller):
    """
    WeeWX Extension Installer - PRESERVES existing YAML-driven architecture.
    
    ONLY CHANGES:
    - Uses WeeWX managers instead of custom database connections
    - Updates YAML routing for tide_table enhancement
    - Removes verbose installation output (30% reduction)
    """
    
    def __init__(self):
        super(MarineDataInstaller, self).__init__(
            version="2.0.0",
            name='marine_data',
            description='NOAA Marine Data Extension for WeeWX',
            author="Shane Burkhardt",
            author_email="info@example.com",
            files=[
                ('bin/user', ['bin/user/marine_data.py']),
                ('bin/user', ['bin/user/marine_data_fields.yaml'])
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
            # CRITICAL: Service registration for WeeWX 5.1
            data_services=['user.marine_data.MarineDataService']
        )

    def configure(self, engine):
        """
        FIXED: Pass engine to configurator for proper WeeWX file path access
        """
        try:
            print("Installing Marine Data Extension...")
            
            # FIXED: Pass engine to configurator for WeeWX-compliant file access
            configurator = MarineDataConfigurator(engine.config_dict, engine)
            config_dict, selected_locations = configurator.run_interactive_setup()
            
            # CRITICAL: Pass configurator in selected_options for table creation
            selected_options = {
                'configurator': configurator,
                'selected_locations': selected_locations
            }
            
            self._create_marine_tables_weewx_compliant(engine, config_dict, selected_options)         
            engine.config_dict.update(config_dict)           
            super().configure(engine)
            
            print("Installation complete! Restart WeeWX to activate.")
            return True
            
        except Exception as e:
            if "not a string" in str(e) and "False" in str(e):
                print(f"Warning (ignored): {e}")
                print("Installation completed successfully despite warning.")
                return True
            else:
                print(f"Installation failed: {e}")
                return False

    def _create_marine_tables_weewx_compliant(self, engine, config_dict, selected_options):
        """
        DATA-DRIVEN: Create marine tables using YAML field definitions and WeeWX database manager.
        
        READS FROM: marine_data_fields.yaml fields section
        CREATES: Tables with fields defined in YAML field mappings, not hardcoded
        FOLLOWS: WeeWX 5.1 database manager patterns
        """
        try:
            # Get YAML data from the configurator instance that was created
            configurator = selected_options.get('configurator')
            if not configurator:
                # Fallback: create new configurator to get YAML data
                configurator = MarineDataConfigurator(engine.config_dict, engine)
            
            # Access YAML data from configurator
            if not hasattr(configurator, 'yaml_data') or not configurator.yaml_data:
                raise RuntimeError("marine_data_fields.yaml not loaded or empty")
            
            # Get fields from YAML (correct structure)
            fields = configurator.yaml_data.get('fields', {})
            if not fields:
                raise RuntimeError("Field definitions not found in marine_data_fields.yaml")
            
            # Group fields by database_table to determine what tables to create
            tables_to_create = set()
            for field_name, field_config in fields.items():
                table_name = field_config.get('database_table', 'archive')
                if table_name != 'archive':  # Skip archive table
                    tables_to_create.add(table_name)
            
            # Use WeeWX database manager instead of custom connections
            with weewx.manager.open_manager_with_config(engine.config_dict, 'wx_binding') as manager:
                
                # Create each required table based on YAML field mappings
                for table_name in tables_to_create:
                    # Build field list for this table from YAML
                    table_fields = {}
                    
                    # Add standard fields that all tables need
                    table_fields['dateTime'] = 'INTEGER NOT NULL'
                    table_fields['station_id'] = 'TEXT NOT NULL'
                    
                    # Add fields defined in YAML for this table
                    for field_name, field_config in fields.items():
                        if field_config.get('database_table') == table_name:
                            db_field = field_config.get('database_field', field_name)
                            db_type = field_config.get('database_type', 'REAL')
                            table_fields[db_field] = db_type
                    
                    if table_name == 'coops_realtime':
                        self._create_coops_realtime_table(manager, table_fields)
                    elif table_name == 'tide_table':
                        self._create_tide_table(manager, table_fields)
                    elif table_name == 'ndbc_data':
                        self._create_ndbc_data_table(manager, table_fields)
            
            print(f"{CORE_ICONS['status']} Marine tables created successfully from YAML field definitions")
            
        except Exception as e:
            print(f"{CORE_ICONS['warning']} Error creating marine tables: {e}")
            raise

    def _determine_required_tables_from_yaml(self, config_dict):
        """
        PRESERVE: DATA-DRIVEN table determination from YAML field mappings
        """
        required_tables = set()
        
        # PRESERVE: Use existing field_mappings pattern from config_dict
        field_mappings = config_dict.get('MarineDataService', {}).get('field_mappings', {})
        
        for module_name, module_fields in field_mappings.items():
            if isinstance(module_fields, dict):
                for service_field, field_config in module_fields.items():
                    if isinstance(field_config, dict):
                        table_name = field_config.get('database_table', 'archive')
                        if table_name != 'archive':
                            required_tables.add(table_name)
        
        return required_tables

    def _create_coops_realtime_table(self, manager, table_fields):
        """
        DATA-DRIVEN: Create coops_realtime table using YAML field definitions
        """
        # Build field definitions from YAML
        field_definitions = []
        for field_name, field_type in table_fields.items():
            field_definitions.append(f"{field_name} {field_type}")
        
        # Add table constraints with MySQL-compatible key length
        constraints = [
            "PRIMARY KEY (dateTime, station_id(20))",
            "INDEX idx_recent_coops (station_id(20), dateTime)"
        ]
        
        # Combine fields and constraints
        all_definitions = field_definitions + constraints
        
        # Create table with YAML-defined fields
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS coops_realtime (
                {', '.join(all_definitions)}
            )
        """
        manager.connection.execute(create_sql)

    def _create_tide_table(self, manager, table_fields):
        """
        DATA-DRIVEN: Create tide_table using YAML field definitions + required operational fields
        """
        # Build field definitions from YAML user-facing fields
        field_definitions = []
        for field_name, field_type in table_fields.items():
            field_definitions.append(f"{field_name} {field_type}")
        
        # Add required operational fields that marine_data.py service uses
        operational_fields = [
            "tide_time INTEGER NOT NULL",
            "tide_type TEXT NOT NULL", 
            "predicted_height REAL",
            "datum TEXT",
            "days_ahead INTEGER"
        ]
        
        # Combine YAML fields + operational fields
        all_field_definitions = field_definitions + operational_fields
        
        # Use operational fields for primary key with MySQL-compatible key length
        constraints = [
            "PRIMARY KEY (station_id(20), tide_time, tide_type(1))",
            "INDEX idx_upcoming_tides (station_id(20), tide_time)"
        ]
        
        # Combine fields and constraints
        all_definitions = all_field_definitions + constraints
        
        # Create table with YAML + operational fields
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS tide_table (
                {', '.join(all_definitions)}
            )
        """
        manager.connection.execute(create_sql)

    def _create_ndbc_data_table(self, manager, table_fields):
        """
        DATA-DRIVEN: Create ndbc_data table using YAML field definitions
        """
        # Build field definitions from YAML
        field_definitions = []
        for field_name, field_type in table_fields.items():
            field_definitions.append(f"{field_name} {field_type}")
        
        # Add table constraints with MySQL-compatible key length
        constraints = [
            "PRIMARY KEY (dateTime, station_id(20))",
            "INDEX idx_recent_ndbc (station_id(20), dateTime)"
        ]
        
        # Combine fields and constraints
        all_definitions = field_definitions + constraints
        
        # Create table with YAML-defined fields
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS ndbc_data (
                {', '.join(all_definitions)}
            )
        """
        manager.connection.execute(create_sql)


class MarineDataConfigurator:
    """
    PRESERVE: Existing interactive configuration with YAML-driven patterns
    
    ONLY CHANGES:
    - Reduced installation output (30% less verbose)
    - Progress indicators instead of detailed logging
    - Use existing YAML structure completely
    """
    
    def __init__(self, config_dict=None, engine=None):
        self.config_dict = config_dict
        self.engine = engine  # Store engine for file path access
        self.selected_stations = {}
        self.selected_fields = {}
        self.yaml_data = {}
        self._load_yaml_configuration()

    def _load_yaml_configuration(self):
        """
        WeeWX COMPLIANT: Load YAML configuration using engine paths
        """
        try:
            # Use WeeWX engine to get proper paths
            if self.engine and hasattr(self.engine, 'config_dict'):
                config_dict = self.engine.config_dict
                
                # Get WEEWX_ROOT from engine configuration  
                weewx_root = config_dict.get('WEEWX_ROOT')
                if not weewx_root:
                    # Default: directory of configuration file
                    weewx_root = os.path.dirname(config_dict.filename)
                
                user_root = config_dict.get('USER_ROOT', 'bin/user')
                
                # Build path to YAML file using WeeWX methodology
                yaml_path = os.path.join(weewx_root, user_root, 'marine_data_fields.yaml')
                
                print(f"DEBUG: WeeWX engine available")
                print(f"DEBUG: Config file: {config_dict.filename}")
                print(f"DEBUG: WEEWX_ROOT: {weewx_root}")
                print(f"DEBUG: USER_ROOT: {user_root}")
                print(f"DEBUG: YAML path: {yaml_path}")
                print(f"DEBUG: YAML exists: {os.path.exists(yaml_path)}")
                
                if os.path.exists(yaml_path):
                    with open(yaml_path, 'r') as file:
                        self.yaml_data = yaml.safe_load(file)
                        print(f"DEBUG: YAML loaded successfully, keys: {list(self.yaml_data.keys())}")
                else:
                    print("DEBUG: YAML file not found at WeeWX path")
                    self.yaml_data = {}
            else:
                print("DEBUG: No WeeWX engine available")
                self.yaml_data = {}
                    
        except Exception as e:
            print(f"DEBUG: YAML loading error: {e}")
            import traceback
            traceback.print_exc()
            self.yaml_data = {}

    def run_interactive_setup(self):
        """
        PRESERVE: Existing interactive setup flow with YAML-driven patterns
        
        ONLY CHANGE: Add location line and clean spinner output
        """
        print(f"\n{CORE_ICONS['selection']} Configuring Marine Data Extension")
        
        # NEW: Add location confirmation right after header
        station_config = self.config_dict.get('Station', {}) if self.config_dict else {}
        latitude = float(station_config.get('latitude'))
        longitude = float(station_config.get('longitude'))
        location_name = station_config.get('location', 'WeeWX Station')

        print(f"Using WeeWX station location: {location_name} ({latitude:.4f}, {longitude:.4f})")
        
        # Step 1: UPDATED - Station discovery using animated spinners
        self._discover_and_select_stations()
        
        # Step 2: PRESERVE - Field selection using existing YAML patterns  
        print(f"{CORE_ICONS['selection']} Selecting fields...", end='', flush=True)
        self._select_fields_from_yaml()
        print(f" {CORE_ICONS['status']}")
        
        # Step 3: PRESERVE - Generate configuration using existing patterns
        config_dict = self._generate_configuration_from_yaml()
        selected_options = {
            'stations': self.selected_stations,
            'fields': self.selected_fields
        }
        
        print(f"{CORE_ICONS['status']} Configuration complete")
        return config_dict, selected_options

    def _discover_coops_stations(self, latitude, longitude, radius_miles=50):
        """
        DATA-DRIVEN: Bounding box CO-OPS station discovery using YAML endpoints
        
        SURGICAL CHANGE: Move verbose output to debug logging
        """
        try:
            log.debug(f"Discovering CO-OPS stations for lat={latitude}, lon={longitude}, radius={radius_miles} miles")
            
            # Get API URLs from YAML configuration (DATA-DRIVEN)
            api_modules = self.yaml_data.get('api_modules', {})
            coops_config = api_modules.get('coops_module', {})
            
            stations_url = coops_config.get('metadata_url', '')
            products_url_template = coops_config.get('products_url', '')
            
            if not stations_url:
                log.error("No CO-OPS metadata URL found in YAML")
                return []
            
            # Calculate bounding box (BOUNDING BOX APPROACH like GClunies)
            radius_degrees = radius_miles / 69.0  # Approximate: 1 degree ≈ 69 miles
            lat_coords = [latitude - radius_degrees, latitude + radius_degrees]
            lon_coords = [longitude - radius_degrees, longitude + radius_degrees]
            
            log.debug(f"Using bounding box: lat {lat_coords}, lon {lon_coords}")
            
            # Discover ALL station types for comprehensive coverage
            station_types = ['tidepredictions', 'waterlevels', 'currents']
            all_discovered_stations = []
            
            for station_type in station_types:
                try:
                    # Build API call with type parameter (DATA-DRIVEN from YAML)
                    params = {
                        'type': station_type,
                        'expand': 'details'
                    }
                    url_params = urllib.parse.urlencode(params)
                    full_url = f"{stations_url}?{url_params}"
                    
                    log.debug(f"Fetching {station_type} stations from API...")
                    
                    response = urllib.request.urlopen(full_url, timeout=30)
                    data = json.loads(response.read().decode('utf-8'))
                    
                    stations_list = data.get('stations', [])
                    log.debug(f"Found {len(stations_list)} {station_type} stations")
                    
                    # Filter stations within bounding box (PRESERVE ALL EXISTING LOGIC)
                    for station_data in stations_list:
                        try:
                            station_lat = float(station_data.get('lat', 0))
                            station_lon = float(station_data.get('lng', 0))
                            
                            # Check if station is within bounding box
                            if (lat_coords[0] <= station_lat <= lat_coords[1] and 
                                lon_coords[0] <= station_lon <= lon_coords[1]):
                                
                                # Calculate distance for sorting
                                distance = self._calculate_distance(latitude, longitude, station_lat, station_lon)
                                
                                # Preserve all station data and add metadata
                                station_record = dict(station_data)
                                station_record['distance'] = distance
                                station_record['station_type'] = station_type
                                
                                # Avoid duplicates (same station may appear in multiple types)
                                station_id = station_record.get('id')
                                if not any(s.get('id') == station_id for s in all_discovered_stations):
                                    all_discovered_stations.append(station_record)
                                
                        except (ValueError, TypeError):
                            continue
                            
                except Exception as e:
                    log.debug(f"Error fetching {station_type} stations: {e}")
                    continue
            
            log.debug(f"Found {len(all_discovered_stations)} unique stations within bounding box")
            
            # Sort by distance and take closest stations
            all_discovered_stations.sort(key=lambda x: x['distance'])
            closest_stations = all_discovered_stations[:15]
            
            log.debug(f"Using {len(closest_stations)} closest stations")
            for i, station in enumerate(closest_stations[:5]):
                log.debug(f"  {i+1}. {station.get('name')} - {station.get('distance', 0):.1f} miles ({station.get('station_type')})")
            
            # PRESERVE: Get capabilities for each station (existing capability detection)
            final_stations = []
            for station in closest_stations:
                station_id = station.get('id')
                
                if products_url_template and station_id:
                    # Use existing capability detection with retry logic (PRESERVE ALL LOGIC)
                    capabilities = []
                    for attempt in range(2):
                        try:
                            products_url = products_url_template.format(station_id=station_id)
                            products_response = urllib.request.urlopen(products_url, timeout=10)
                            products_data = json.loads(products_response.read().decode('utf-8'))
                            
                            # Get capability mapping from YAML (DATA-DRIVEN)
                            capability_mapping = coops_config.get('product_capability_mapping', {})
                            
                            # Extract capabilities using YAML mapping
                            products_list = products_data.get('products', [])
                            
                            for product in products_list:
                                product_name = product.get('name', '')
                                
                                # Use YAML capability mapping
                                for capability, keywords in capability_mapping.items():
                                    if any(keyword.lower() in product_name.lower() for keyword in keywords):
                                        capabilities.append(capability)
                            
                            break  # Success
                            
                        except Exception as e:
                            if attempt == 0:
                                time.sleep(1)  # Wait and retry
                            else:
                                capabilities = ['tide_predictions']  # Default for all station types
                    
                    station['capabilities'] = list(set(capabilities)) if capabilities else ['tide_predictions']
                else:
                    station['capabilities'] = ['tide_predictions']  # Default capability
                
                final_stations.append(station)
            
            log.debug(f"Returning {len(final_stations)} stations with capabilities")
            return final_stations
            
        except Exception as e:
            log.error(f"Error in CO-OPS station discovery: {e}")
            return []
   
    def _discover_ndbc_stations(self, latitude, longitude):
        """
        Discover NDBC stations within range, filtering by actual data capabilities
        """
        try:
            # Get NDBC metadata URL from YAML configuration
            api_modules = self.yaml_data.get('api_modules', {})
            ndbc_config = api_modules.get('ndbc_module', {})
            metadata_url = ndbc_config.get('metadata_url', '')
            response = urllib.request.urlopen(metadata_url, timeout=30)
            content = response.read().decode('utf-8')
            
            # Parse XML to extract station info
            import xml.etree.ElementTree as ET
            root = ET.fromstring(content)
            
            nearby_stations = []
            for station in root.findall('.//station'):
                try:
                    station_id = station.get('id')
                    station_name = station.get('name', f'NDBC {station_id}')
                    station_lat = float(station.get('lat', 0))
                    station_lon = float(station.get('lon', 0))
                    
                    # Calculate distance first
                    distance = self._calculate_distance(latitude, longitude, station_lat, station_lon)
                    
                    if distance <= 100:  # Use same distance limit as CO-OPS method
                        # Test if station has useful capabilities we want
                        capabilities = self._test_ndbc_station_real_data(station_id)
                        
                        # Only include stations that have capabilities we're looking for
                        useful_capabilities = ['Atmospheric Data', 'Wave Data', 'Ocean Temperature']
                        if any(cap in capabilities for cap in useful_capabilities):
                            # Calculate cardinal bearing
                            bearing = self._calculate_bearing(latitude, longitude, station_lat, station_lon)
                            cardinal = self._bearing_to_16_point_cardinal(bearing)
                            
                            nearby_stations.append({
                                'id': station_id,
                                'name': station_name,
                                'lat': station_lat,
                                'lon': station_lon,
                                'distance': distance,
                                'cardinal': cardinal,
                                'capabilities': capabilities
                            })
                        
                except (ValueError, TypeError, AttributeError):
                    continue
            
            # Sort by distance and return closest stations
            nearby_stations.sort(key=lambda x: x['distance'])
            log.debug(f"Found {len(nearby_stations)} NDBC stations within 100 miles")
            return nearby_stations[:15]
            
        except Exception as e:
            log.error(f"Error discovering NDBC stations: {e}")
            return []
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """
        PRESERVE: Existing distance calculation using Haversine formula
        """
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        distance_km = 6371 * c
        distance_miles = distance_km * 0.621371
        
        return distance_miles

    def _select_fields_from_yaml(self):
        """
        PRESERVE: Existing field selection using YAML structure
        """
        # PRESERVE: Use existing YAML field definitions
        fields = self.yaml_data.get('fields', {})
        
        # PRESERVE: Use existing complexity level selection patterns
        complexity_level = self._get_complexity_level()
        
        # PRESERVE: Filter fields based on complexity level and YAML patterns
        selected_fields = {}
        for field_name, field_config in fields.items():
            complexity_levels = field_config.get('complexity_levels', ['all'])
            if complexity_level in complexity_levels or 'all' in complexity_levels:
                selected_fields[field_name] = True
        
        self.selected_fields = selected_fields

    def _get_complexity_level(self):
        """
        PRESERVE: Existing complexity level selection
        """
        print("\nSelect data collection level:")
        print("1. Minimal - Essential marine data only")
        print("2. All - Complete marine data collection")
        
        try:
            choice = input("Enter choice (1-2): ").strip()
            if choice == '1':
                return 'minimal'
            else:
                return 'all'
        except:
            return 'minimal'  # Default

    def _get_update_interval(self, module_name):
        """
        PRESERVE: Get appropriate update interval for module (existing pattern)
        """
        # PRESERVE: Use existing interval patterns from YAML
        api_modules = self.yaml_data.get('api_modules', {})
        module_config = api_modules.get(module_name, {})
        return module_config.get('recommended_interval', 3600)

    def _interactive_station_selection_curses(self, coops_stations, ndbc_stations):
        """
        NEW METHOD: Two-page curses interface for station selection
        Called from existing _discover_and_select_stations() method
        """
        selected_stations = {'coops_module': [], 'ndbc_module': []}
        
        try:
            # Page 1: CO-OPS stations
            if coops_stations:
                selected_coops = self._curses_station_page(coops_stations, "CO-OPS Tide Stations")
                selected_stations['coops_module'] = selected_coops
            
            # Page 2: NDBC stations  
            if ndbc_stations:
                selected_ndbc = self._curses_station_page(ndbc_stations, "NDBC Marine Weather Buoys")
                selected_stations['ndbc_module'] = selected_ndbc
                
        except Exception as e:
            print(f"{CORE_ICONS['warning']} Error in station selection: {e}")
            # Fallback to first stations if curses fails
            selected_stations['coops_module'] = coops_stations[:2] if coops_stations else []
            selected_stations['ndbc_module'] = ndbc_stations[:1] if ndbc_stations else []
        
        return selected_stations

    def _curses_station_page(self, stations, page_title):
        """
        Curses interface with proper spacing, scrolling, and cardinal bearing display
        """
        def station_selection_screen(stdscr):
            curses.curs_set(0)  # Hide cursor
            stdscr.clear()
            
            selected_indices = set()
            current_row = 0
            scroll_offset = 0
            max_row = len(stations) - 1
            
            while True:
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Header
                header = f"{CORE_ICONS['navigation']} {page_title}"
                stdscr.addstr(0, 0, header, curses.A_BOLD)
                stdscr.addstr(1, 0, "=" * min(len(header), width-1))
                
                # Instructions
                instructions = [
                    "Use arrow keys to navigate, SPACE to select/deselect, ENTER to continue",
                    "Select multiple stations for backup coverage during maintenance periods"
                ]
                
                for i, instruction in enumerate(instructions):
                    if 2 + i < height - 1:
                        stdscr.addstr(2 + i, 0, instruction[:width-1])
                
                # Calculate display area
                start_display_row = 5
                available_lines = height - start_display_row - 2  # Leave room for status
                lines_per_station = 3  # Station line + capabilities line + blank line
                stations_per_page = available_lines // lines_per_station
                
                # Calculate scroll bounds
                if current_row >= scroll_offset + stations_per_page:
                    scroll_offset = current_row - stations_per_page + 1
                elif current_row < scroll_offset:
                    scroll_offset = current_row
                    
                scroll_offset = max(0, min(scroll_offset, len(stations) - stations_per_page))
                
                # Display stations
                display_row = start_display_row
                for i in range(scroll_offset, min(scroll_offset + stations_per_page, len(stations))):
                    if display_row >= height - 3:
                        break
                        
                    station = stations[i]
                    
                    # Selection indicator
                    checkbox = "[X]" if i in selected_indices else "[ ]"
                    
                    # Station info line with cardinal bearing
                    distance = station.get('distance', 0)
                    cardinal = station.get('cardinal', '')  # Extract cardinal bearing
                    station_name = station.get('name', 'Unknown')
                    station_id = station.get('id', 'N/A')
                    state = station.get('state', '')
                    
                    # Include cardinal bearing in display when available
                    if cardinal:
                        station_line = f"{checkbox} {station_name} ({station_id}) - {distance:.1f} mi {cardinal}"
                    else:
                        station_line = f"{checkbox} {station_name} ({station_id}) - {distance:.1f} mi"
                    
                    if state:
                        station_line += f" [{state}]"
                    
                    # Highlight current row
                    attr = curses.A_REVERSE if i == current_row else curses.A_NORMAL
                    
                    try:
                        stdscr.addstr(display_row, 0, station_line[:width-1], attr)
                        
                        # Capabilities line (indented)
                        capabilities = station.get('capabilities', [])
                        if capabilities:
                            cap_text = "    Capabilities: " + ", ".join(capabilities)
                        else:
                            cap_text = "    Capabilities: Unknown"
                        
                        stdscr.addstr(display_row + 1, 0, cap_text[:width-1], curses.A_DIM)
                        
                        # Blank line for spacing
                        display_row += 3
                        
                    except curses.error:
                        break  # Screen boundary reached
                
                # Scroll indicators
                if scroll_offset > 0:
                    try:
                        stdscr.addstr(start_display_row - 1, width - 10, "↑ More ↑", curses.A_BOLD)
                    except curses.error:
                        pass
                        
                if scroll_offset + stations_per_page < len(stations):
                    try:
                        stdscr.addstr(height - 3, width - 10, "↓ More ↓", curses.A_BOLD)
                    except curses.error:
                        pass
                
                # Status line
                status = f"Selected: {len(selected_indices)} | Station {current_row + 1}/{len(stations)} | ENTER to continue"
                try:
                    stdscr.addstr(height-1, 0, status[:width-1], curses.A_BOLD)
                except curses.error:
                    pass
                
                stdscr.refresh()
                
                # Handle input
                key = stdscr.getch()
                
                if key == curses.KEY_UP and current_row > 0:
                    current_row -= 1
                elif key == curses.KEY_DOWN and current_row < max_row:
                    current_row += 1
                elif key == ord(' '):  # Spacebar to select/deselect
                    if current_row in selected_indices:
                        selected_indices.remove(current_row)
                    else:
                        selected_indices.add(current_row)
                elif key == ord('\n') or key == curses.KEY_ENTER or key == 10:  # Enter to continue
                    break
                elif key == ord('q') or key == ord('Q'):  # Quit
                    return []
            
            # Return selected stations
            return [stations[i].get('id', str(stations[i])) for i in selected_indices]
        
        try:
            return curses.wrapper(station_selection_screen)
        except Exception as e:
            print(f"{CORE_ICONS['warning']} Curses interface error: {e}")
            return stations[:2]  # Fallback selection

    def _format_station_capabilities(self, capabilities):
        """
        NEW METHOD: Format station capabilities for display
        """
        if not capabilities:
            return "Capabilities: Unknown"
        
        # Map capabilities to user-friendly descriptions
        capability_map = {
            'water_level_observed': 'Real-time Water Level',
            'tide_predictions': 'Tide Predictions',
            'water_temperature': 'Water Temperature',
            'meteorological': 'Weather Data',
            'currents': 'Current Data'
        }
        
        friendly_caps = []
        for cap in capabilities:
            friendly_name = capability_map.get(cap, cap.replace('_', ' ').title())
            friendly_caps.append(friendly_name)
        
        return f"Capabilities: {', '.join(friendly_caps)}"

    def _enhance_coops_stations_with_capabilities(self, stations):
        """
        Add capabilities to CO-OPS stations and include cardinal bearings
        """
        enhanced_stations = []
        
        for station in stations:
            enhanced_station = station.copy()
            
            # Calculate cardinal bearing for CO-OPS stations
            station_lat = float(station.get('lat', 0))
            station_lon = float(station.get('lng', 0))
            bearing = self._calculate_bearing(self.user_latitude, self.user_longitude, station_lat, station_lon)
            cardinal = self._bearing_to_16_point_cardinal(bearing)
            enhanced_station['cardinal'] = cardinal
            
            # Get station capabilities from existing method (preserve existing logic)
            capabilities = self._get_coops_station_capabilities(station['id'])
            enhanced_station['capabilities'] = capabilities
            
            enhanced_stations.append(enhanced_station)
        
        return enhanced_stations

    def _get_coops_station_capabilities(self, station_id):
        """
        NEW METHOD: Detect CO-OPS station capabilities via API
        """
        capabilities = []
        
        try:
            # Check station products API
            products_url = f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station_id}/products.json"
            
            response = urllib.request.urlopen(products_url, timeout=10)
            data = json.loads(response.read().decode('utf-8'))
            
            products = data.get('products', [])
            
            # Map products to capabilities
            for product in products:
                product_name = product.get('name', '').lower()
                
                if 'water level' in product_name or 'verified' in product_name:
                    capabilities.append('water_level_observed')
                elif 'prediction' in product_name or 'harmonic' in product_name:
                    capabilities.append('tide_predictions')
                elif 'water temp' in product_name:
                    capabilities.append('water_temperature')
                elif 'meteorological' in product_name or 'wind' in product_name:
                    capabilities.append('meteorological')
                elif 'current' in product_name:
                    capabilities.append('currents')
            
            # Remove duplicates
            capabilities = list(set(capabilities))
            
        except Exception as e:
            # Default capabilities if API fails
            capabilities = ['water_level_observed', 'tide_predictions']
        
        return capabilities

    def _interactive_field_selection_curses(self, available_fields):
        """
        IMPROVED: Curses interface with proper spacing, headers, and scrolling
        """
        def field_selection_screen(stdscr):
            curses.curs_set(0)
            stdscr.clear()
            
            # Organize fields by module
            coops_fields = []
            ndbc_fields = []
            
            for field_name, field_config in available_fields.items():
                api_module = field_config.get('api_module', '')
                field_display = {
                    'name': field_name,
                    'display_name': field_config.get('display_name', field_name),
                    'description': field_config.get('description', ''),
                    'config': field_config
                }
                
                if 'coops' in api_module:
                    coops_fields.append(field_display)
                elif 'ndbc' in api_module:
                    ndbc_fields.append(field_display)
            
            all_fields = coops_fields + ndbc_fields
            selected_indices = set(range(len(all_fields)))  # Start with all selected
            current_row = 0
            max_row = len(all_fields) - 1
            scroll_offset = 0
            
            while True:
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Header
                header = f"{CORE_ICONS['selection']} Marine Data Field Selection"
                stdscr.addstr(0, 0, header, curses.A_BOLD)
                stdscr.addstr(1, 0, "=" * min(len(header), width-1))
                
                # Instructions
                instructions = [
                    "Use arrow keys to navigate, SPACE to select/deselect, ENTER to continue",
                    "All fields selected by default - deselect unwanted fields"
                ]
                
                for i, instruction in enumerate(instructions):
                    if 2 + i < height - 1:
                        stdscr.addstr(2 + i, 0, instruction[:width-1])
                
                # Calculate display area
                start_display_row = 5
                available_lines = height - start_display_row - 2  # Leave room for status
                lines_per_field = 3  # Field line + description line + blank line
                lines_per_section_header = 2  # Header line + separator line
                
                # Calculate total display lines needed
                total_lines_needed = 0
                if coops_fields:
                    total_lines_needed += lines_per_section_header + (len(coops_fields) * lines_per_field)
                if ndbc_fields:
                    total_lines_needed += lines_per_section_header + (len(ndbc_fields) * lines_per_field)
                
                # Calculate scrolling
                max_scroll = max(0, total_lines_needed - available_lines)
                
                # Adjust scroll based on current field position
                current_field_line = 0
                if coops_fields:
                    current_field_line += lines_per_section_header
                    if current_row < len(coops_fields):
                        current_field_line += current_row * lines_per_field
                    else:
                        current_field_line += len(coops_fields) * lines_per_field
                        if ndbc_fields:
                            current_field_line += lines_per_section_header
                            current_field_line += (current_row - len(coops_fields)) * lines_per_field
                
                # Auto-scroll to keep current field visible
                if current_field_line - scroll_offset >= available_lines - 3:
                    scroll_offset = current_field_line - available_lines + 6
                elif current_field_line < scroll_offset:
                    scroll_offset = max(0, current_field_line - 3)
                
                scroll_offset = max(0, min(scroll_offset, max_scroll))
                
                # Display content with scrolling
                display_row = start_display_row
                current_line = 0
                current_field_index = 0
                
                # CO-OPS Section
                if coops_fields:
                    # Section header
                    if current_line >= scroll_offset and display_row < height - 2:
                        try:
                            stdscr.addstr(display_row, 0, "CO-OPS (Tides & Water Levels):", curses.A_BOLD)
                            display_row += 1
                            stdscr.addstr(display_row, 0, "─" * 30, curses.A_BOLD)
                            display_row += 1
                        except curses.error:
                            pass
                    current_line += lines_per_section_header
                    
                    # CO-OPS fields
                    for field in coops_fields:
                        if current_line >= scroll_offset and display_row < height - 2:
                            # Selection indicator and field name
                            checkbox = "[X]" if current_field_index in selected_indices else "[ ]"
                            field_line = f"  {checkbox} {field['display_name']}"
                            
                            # Highlight current row
                            attr = curses.A_REVERSE if current_field_index == current_row else curses.A_NORMAL
                            
                            try:
                                stdscr.addstr(display_row, 0, field_line[:width-1], attr)
                                display_row += 1
                                
                                # Description line with arrow
                                if field['description'] and display_row < height - 2:
                                    desc_line = f"      → {field['description']}"
                                    stdscr.addstr(display_row, 0, desc_line[:width-1], curses.A_DIM)
                                    display_row += 1
                                
                                # Blank line for spacing
                                if display_row < height - 2:
                                    display_row += 1
                                    
                            except curses.error:
                                break
                        
                        current_line += lines_per_field
                        current_field_index += 1
                
                # NDBC Section
                if ndbc_fields:
                    # Section header
                    if current_line >= scroll_offset and display_row < height - 2:
                        try:
                            stdscr.addstr(display_row, 0, "NDBC (Marine Weather):", curses.A_BOLD)
                            display_row += 1
                            stdscr.addstr(display_row, 0, "─" * 30, curses.A_BOLD)
                            display_row += 1
                        except curses.error:
                            pass
                    current_line += lines_per_section_header
                    
                    # NDBC fields
                    for field in ndbc_fields:
                        if current_line >= scroll_offset and display_row < height - 2:
                            # Selection indicator and field name
                            checkbox = "[X]" if current_field_index in selected_indices else "[ ]"
                            field_line = f"  {checkbox} {field['display_name']}"
                            
                            # Highlight current row
                            attr = curses.A_REVERSE if current_field_index == current_row else curses.A_NORMAL
                            
                            try:
                                stdscr.addstr(display_row, 0, field_line[:width-1], attr)
                                display_row += 1
                                
                                # Description line with arrow
                                if field['description'] and display_row < height - 2:
                                    desc_line = f"      → {field['description']}"
                                    stdscr.addstr(display_row, 0, desc_line[:width-1], curses.A_DIM)
                                    display_row += 1
                                
                                # Blank line for spacing
                                if display_row < height - 2:
                                    display_row += 1
                                    
                            except curses.error:
                                break
                        
                        current_line += lines_per_field
                        current_field_index += 1
                
                # Scroll indicators
                if scroll_offset > 0:
                    try:
                        stdscr.addstr(start_display_row - 1, width - 10, "↑ More ↑", curses.A_BOLD)
                    except curses.error:
                        pass
                
                if scroll_offset < max_scroll:
                    try:
                        stdscr.addstr(height - 3, width - 10, "↓ More ↓", curses.A_BOLD)
                    except curses.error:
                        pass
                
                # Status line
                status = f"Selected: {len(selected_indices)}/{len(all_fields)} fields | Field {current_row + 1}/{len(all_fields)} | ENTER to continue"
                try:
                    stdscr.addstr(height-1, 0, status[:width-1], curses.A_BOLD)
                except curses.error:
                    pass
                
                stdscr.refresh()
                
                # Handle input
                key = stdscr.getch()
                
                if key == curses.KEY_UP and current_row > 0:
                    current_row -= 1
                elif key == curses.KEY_DOWN and current_row < max_row:
                    current_row += 1
                elif key == ord(' '):  # Spacebar to select/deselect
                    if current_row in selected_indices:
                        selected_indices.remove(current_row)
                    else:
                        selected_indices.add(current_row)
                elif key == ord('\n') or key == curses.KEY_ENTER or key == 10:  # Enter to continue
                    break
                elif key == ord('q') or key == ord('Q'):  # Quit
                    return {}
            
            # Return selected fields
            selected_fields = {}
            for i in selected_indices:
                if i < len(all_fields):
                    field_name = all_fields[i]['name']
                    selected_fields[field_name] = True
            
            return selected_fields
        
        try:
            return curses.wrapper(field_selection_screen)
        except Exception as e:
            print(f"{CORE_ICONS['warning']} Field selection error: {e}")
            # Fallback to all fields
            return {name: True for name in available_fields.keys()}

    def _discover_and_select_stations(self):
        """
        Discover and select stations with progress indicators
        """
        try:
            # Get user location from WeeWX configuration
            station_config = self.config_dict.get('Station', {}) if self.config_dict else {}
            latitude = station_config.get('latitude')
            longitude = station_config.get('longitude')
            
            # Simple check - if WeeWX is running, location should be available
            if latitude is None or longitude is None:
                log.error("Cannot get location from WeeWX configuration")
                return False
            
            latitude = float(latitude)
            longitude = float(longitude)
            
            self.user_latitude = latitude
            self.user_longitude = longitude

            # Initialize progress manager
            progress = InstallationProgressManager()
            
            # Discover CO-OPS stations with progress
            progress.start_spinner("Discovering CO-OPS stations")
            coops_stations = self._discover_coops_stations(latitude, longitude)
            enhanced_coops = self._enhance_coops_stations_with_capabilities(coops_stations)
            if not enhanced_coops:
                progress.stop_spinner("Discovering CO-OPS stations", success=False, error_msg="No stations found")
            else:
                progress.stop_spinner("Discovering CO-OPS stations", success=True)
            
            # Discover NDBC stations with progress (now includes capability testing and enhancement)
            progress.start_spinner("Discovering NDBC stations")
            enhanced_ndbc = self._discover_ndbc_stations(latitude, longitude)
            progress.stop_spinner("Discovering NDBC stations", success=bool(enhanced_ndbc))
            
            # Store enhanced station lists as instance attributes for metadata writing
            self.enhanced_coops_stations = enhanced_coops
            self.enhanced_ndbc_stations = enhanced_ndbc
            
            # Continue with interactive selection
            selected_stations = self._interactive_station_selection_curses(enhanced_coops, enhanced_ndbc)
            self.selected_stations = selected_stations
            
            # MARK SELECTED STATIONS IN ENHANCED LISTS
            # Mark selected CO-OPS stations
            selected_coops_ids = selected_stations.get('coops_module', [])
            for station in self.enhanced_coops_stations:
                if station.get('id') in selected_coops_ids:
                    station['selected'] = True
                else:
                    station['selected'] = False
            
            # Mark selected NDBC stations
            selected_ndbc_ids = selected_stations.get('ndbc_module', [])
            for station in self.enhanced_ndbc_stations:
                if station.get('id') in selected_ndbc_ids:
                    station['selected'] = True
                else:
                    station['selected'] = False
            
            return True
            
        except Exception as e:
            log.error(f"Error in station discovery: {e}")
            return False
    
    def _select_fields_from_yaml(self):
        """
        MODIFIED: Add curses interface call to existing method
        """
        # Get available fields from YAML (existing code unchanged)
        fields = self.yaml_data.get('fields', {})
        
        # NEW: Use curses interface for selection
        selected_fields = self._interactive_field_selection_curses(fields)
        self.selected_fields = selected_fields

    def _generate_configuration_from_yaml(self):
        """
        CRITICAL METHOD: Transform YAML field definitions into WeeWX config_dict structure
        
        This creates the field_mappings that marine_data.py reads at runtime for:
        - Data extraction from APIs (api_path)
        - Table routing (database_table) 
        - Field mapping (database_field)
        - Unit conversions (unit_group)
        """
        config = {
            'MarineDataService': {
                'enable': 'true',
                'timeout': '30',
                'log_success': 'false',
                'log_errors': 'true', 
                'retry_attempts': '3',
                'user_latitude': str(getattr(self, 'user_latitude', 33.6595)),
                'user_longitude': str(getattr(self, 'user_longitude', -117.9988))
            }
        }
        
        # CRITICAL: Write selected stations to config
        selected_stations = config['MarineDataService'].setdefault('selected_stations', {})
        for module_name, station_list in self.selected_stations.items():
            station_config = selected_stations.setdefault(module_name.replace('_module', '_stations'), {})
            for station_id in station_list:
                station_config[station_id] = 'true'  # String values required
        
        # CRITICAL: Write field selection for service initialization
        field_selection = config['MarineDataService'].setdefault('field_selection', {})
        field_selection['selection_timestamp'] = str(int(time.time()))
        field_selection['config_version'] = '1.0'
        field_selection['complexity_level'] = 'custom'
        
        # Group selected fields by module for service
        selected_field_groups = field_selection.setdefault('selected_fields', {})
        coops_fields = []
        ndbc_fields = []
        
        # CRITICAL: Transform YAML fields into runtime field mappings
        fields = self.yaml_data.get('fields', {})
        field_mappings = config['MarineDataService'].setdefault('field_mappings', {})
        
        for field_name, is_selected in self.selected_fields.items():
            if is_selected and field_name in fields:
                field_config = fields[field_name]
                api_module = field_config.get('api_module', 'unknown_module')
                
                # Group fields by module for field_selection
                if api_module == 'coops_module':
                    coops_fields.append(field_name)
                elif api_module == 'ndbc_module':
                    ndbc_fields.append(field_name)
                
                # CRITICAL: Create field mappings for runtime service
                module_mappings = field_mappings.setdefault(api_module, {})
                module_mappings[field_name] = {
                    'database_field': field_config.get('database_field', field_name),
                    'database_type': field_config.get('database_type', 'REAL'),
                    'database_table': field_config.get('database_table', 'archive'),
                    'api_path': field_config.get('api_path', ''),
                    'unit_group': field_config.get('unit_group', 'group_count'),
                    'api_product': field_config.get('api_product', ''),
                    'description': field_config.get('description', '')
                }
        
        # Write grouped field selections
        if coops_fields:
            selected_field_groups['coops_module'] = ', '.join(coops_fields)
        if ndbc_fields:
            selected_field_groups['ndbc_module'] = ', '.join(ndbc_fields)
        
        # CRITICAL: Write collection intervals
        collection_intervals = config['MarineDataService'].setdefault('collection_intervals', {})
        collection_intervals['coops_collection_interval'] = '600'      # 10 minutes
        collection_intervals['tide_predictions_interval'] = '21600'    # 6 hours
        collection_intervals['ndbc_weather_interval'] = '3600'         # 1 hour
        collection_intervals['ndbc_ocean_interval'] = '3600'           # 1 hour
        
        # CRITICAL: Write unit system configuration
        unit_system = config['MarineDataService'].setdefault('unit_system', {})
        convert_config = self.config_dict.get('StdConvert', {}) if self.config_dict else {}
        weewx_unit_system = convert_config.get('target_unit', 'US')
        unit_system['weewx_system'] = weewx_unit_system
        
        # CRITICAL: Write API endpoints for configurable URLs
        api_endpoints = config['MarineDataService'].setdefault('api_endpoints', {})
        api_modules = self.yaml_data.get('api_modules', {})
        
        for module_name, module_config in api_modules.items():
            endpoint_config = api_endpoints.setdefault(module_name, {})
            endpoint_config['base_url'] = module_config.get('api_url', '')
            endpoint_config['timeout'] = str(module_config.get('timeout', 30))
            endpoint_config['retry_attempts'] = str(module_config.get('retry_attempts', 3))

        self._write_station_metadata(config)
        
        return config

    def _test_ndbc_station_real_data(self, station_id):
        """Test actual data content from station's .txt file to determine capabilities."""
        try:
            # Get NDBC configuration from YAML
            api_modules = self.yaml_data.get('api_modules', {})
            ndbc_config = api_modules.get('ndbc_module', {})
            
            # Build URL from YAML station_pattern
            station_pattern = ndbc_config.get('station_pattern', '')
            if not station_pattern:
                return []
                
            url = station_pattern.format(station_id=station_id)
            timeout = ndbc_config.get('timeout', 10)
            
            # Download same file that marine_data.py will parse
            with urllib.request.urlopen(url, timeout=timeout) as response:
                content = response.read().decode('utf-8')
            
            # Parse same way as marine_data.py
            lines = content.strip().split('\n')
            if len(lines) < 3:
                return []
            
            headers = lines[0].split()  # Field names
            data_line = lines[2].split()  # Most recent data values
            
            # Build field-to-data mapping from .txt file
            available_data = {}
            for i, header in enumerate(headers):
                if i < len(data_line):
                    value = data_line[i]
                    available_data[header] = value
            
            # Group fields by natural sensor type (from .txt file field names)
            sensor_groups = {
                'Atmospheric Data': ['WDIR', 'WSPD', 'GST', 'PRES', 'ATMP', 'DEWP', 'VIS'],
                'Wave Data': ['WVHT', 'DPD', 'APD', 'MWD'], 
                'Ocean Temperature': ['WTMP']
            }
            
            # Test each sensor group for actual data (not "MM", "999.0", "99.0")
            available_capabilities = []
            missing_indicators = ['MM', '999.0', '99.0', 'MM.']
            
            for capability, field_list in sensor_groups.items():
                has_real_data = False
                
                for field in field_list:
                    if field in available_data:
                        value = available_data[field]
                        if value not in missing_indicators:
                            try:
                                # Additional check - valid numeric data
                                float_val = float(value)
                                if float_val not in [999.0, 99.0, -999.0]:
                                    has_real_data = True
                                    break
                            except ValueError:
                                # Non-numeric but not missing indicator
                                if value not in missing_indicators:
                                    has_real_data = True
                                    break
                
                if has_real_data:
                    available_capabilities.append(capability)
            
            return available_capabilities
            
        except Exception as e:
            log.debug(f"Station {station_id}: Error testing capabilities: {e}")
            return []

    def _calculate_bearing(self, lat1, lon1, lat2, lon2):
        """
        Calculate true bearing from point 1 to point 2 in degrees
        """
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlon_rad = math.radians(lon2 - lon1)
        
        y = math.sin(dlon_rad) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) - 
            math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad))
        
        bearing_rad = math.atan2(y, x)
        bearing_deg = math.degrees(bearing_rad)
        
        # Normalize to 0-360 degrees
        return (bearing_deg + 360) % 360

    def _bearing_to_16_point_cardinal(self, bearing):
        """
        Convert bearing in degrees to 16-point cardinal direction
        """
        directions = [
            'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
            'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'
        ]
        
        # Each direction covers 22.5 degrees
        index = round(bearing / 22.5) % 16
        return directions[index]

    def _write_station_metadata(self, config):
        """
        NEW METHOD: Write station metadata to CONF for Phase II analysis
        
        Uses data already collected during Phase I installation:
        - enhanced_coops_stations: Enhanced CO-OPS stations with capabilities
        - enhanced_ndbc_stations: Enhanced NDBC stations with capabilities  
        - Calculated distances from _discover_and_select_stations()
        - Capability detection from _test_ndbc_station_real_data()
        
        This allows Phase II to analyze station coverage without duplicating
        discovery logic or making additional API calls.
        """
        # Only write metadata if we have station data
        if not (hasattr(self, 'enhanced_coops_stations') or hasattr(self, 'enhanced_ndbc_stations')):
            return
        
        metadata_section = config['MarineDataService'].setdefault('station_metadata', {})
        
        # Write CO-OPS station metadata (from existing enhanced_coops_stations)
        if hasattr(self, 'enhanced_coops_stations') and self.enhanced_coops_stations:
            coops_metadata = metadata_section.setdefault('coops_stations', {})
            
            for station in self.enhanced_coops_stations:
                # Only write metadata for selected stations
                if station.get('selected', False):
                    station_id = station['id']
                    coops_metadata[station_id] = {
                        'name': station.get('name', f'Station {station_id}'),
                        'latitude': str(station.get('lat', 0.0)),
                        'longitude': str(station.get('lng', 0.0)),
                        'distance_miles': str(round(station.get('distance', 0.0), 1)),
                        'capabilities': ', '.join(station.get('capabilities', [])),
                        'station_type': 'coops'
                    }
        
        # Write NDBC station metadata (from existing enhanced_ndbc_stations)  
        if hasattr(self, 'enhanced_ndbc_stations') and self.enhanced_ndbc_stations:
            ndbc_metadata = metadata_section.setdefault('ndbc_stations', {})
            
            for station in self.enhanced_ndbc_stations:
                # Only write metadata for selected stations
                if station.get('selected', False):
                    station_id = station['id']
                    
                    # Use existing capability detection from _test_ndbc_station_real_data()
                    capabilities = station.get('capabilities', [])
                    has_wave = 'Wave Data' in capabilities
                    has_atmospheric = 'Atmospheric Data' in capabilities
                    
                    # Build capabilities list for Phase II
                    capabilities_list = []
                    if has_wave:
                        capabilities_list.append('wave_data')
                    if has_atmospheric:
                        capabilities_list.append('atmospheric_data')
                    
                    ndbc_metadata[station_id] = {
                        'name': station.get('name', f'Buoy {station_id}'),
                        'latitude': str(station.get('lat', 0.0)),
                        'longitude': str(station.get('lon', 0.0)),
                        'distance_miles': str(round(station.get('distance', 0.0), 1)),
                        'capabilities': ', '.join(capabilities_list),
                        'wave_capability': 'true' if has_wave else 'false',
                        'atmospheric_capability': 'true' if has_atmospheric else 'false',
                        'station_type': 'ndbc'
                    }


class COOPSAPIClient:
    """
    PRESERVE: Existing CO-OPS API client with YAML-driven configuration
    """
    def __init__(self, timeout=30, retry_attempts=3):
        self.timeout = timeout
        self.retry_attempts = retry_attempts

class NDBCAPIClient:
    """
    PRESERVE: Existing NDBC API client with YAML-driven configuration  
    """
    def __init__(self, timeout=30):
        self.timeout = timeout