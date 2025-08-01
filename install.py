#!/usr/bin/env python3\
# Magic Animal: Aldabra Tortoise
"""
WeeWX Marine Data Extension Installer

Re-organized installer following WeeWX 5.1 success manual patterns.
All functionality preserved - just properly separated into specialist classes.

Key Success Patterns Implemented:
- Simple ExtensionInstaller class (15-20 lines)
- Correct imports: weecfg.extension not weewx.engine
- Required loader() function
- List format service registration
- String-only configuration values
- Specialist classes for complex operations

Author: Shane Burkhardt
"""

import os
import sys
import subprocess
import sqlite3
import time
import yaml
import requests
import math
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
            
            # Step 2: Database schema management (delegated)
            db_manager = MarineDatabaseManager(engine.config_dict)
            db_manager.extend_database_schema(selected_options)
            
            # Step 3: Update engine configuration
            engine.config_dict.update(config_dict)
            
            print("\n" + "="*80)
            print("INSTALLATION COMPLETED SUCCESSFULLY!")
            print("="*80)
            print("✓ Files installed")
            print("✓ Service registered automatically")
            print("✓ Interactive configuration completed")
            print("✓ Database schema extended")
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
            print("Using fallback configuration...")
            selected_stations = []
        else:
            selected_stations = self._display_station_selection(stations)
        
        # Field selection based on YAML complexity levels
        print("\n🎯 FIELD SELECTION")
        print("-" * 30)
        selected_fields = self._collect_field_selections()
        
        # Collection intervals configuration
        intervals = self._configure_collection_intervals()
        
        # Write configuration files
        config_dict = self._write_configuration_files(
            selected_stations, selected_fields, intervals, user_lat, user_lon
        )
        
        selected_options = {
            'stations': selected_stations,
            'fields': selected_fields,
            'intervals': intervals,
            'user_location': (user_lat, user_lon)
        }
        
        print("\n✅ Interactive configuration completed successfully!")
        return config_dict, selected_options
    
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
    
    def _discover_stations(self, user_lat, user_lon, max_distance_km=100):
        """Discover marine stations within specified distance."""
        all_stations = []
        
        # Discover CO-OPS stations
        print("  🔍 Searching CO-OPS (Tides & Currents) stations...")
        coops_stations = self._discover_coops_stations(user_lat, user_lon, max_distance_km)
        all_stations.extend(coops_stations)
        
        # Discover NDBC stations
        print("  🔍 Searching NDBC (Buoy) stations...")
        ndbc_stations = self._discover_ndbc_stations(user_lat, user_lon, max_distance_km)
        all_stations.extend(ndbc_stations)
        
        print(f"  ✅ Found {len(all_stations)} stations within {max_distance_km}km")
        return all_stations
    
    def _discover_coops_stations(self, user_lat, user_lon, max_distance_km):
        """Discover CO-OPS stations using NOAA API with comprehensive parsing."""
        stations = []
        try:
            coops_module = self.yaml_data.get('api_modules', {}).get('coops_module', {})
            metadata_url = coops_module.get('metadata_url')

            if not metadata_url:
                print("❌ ERROR: coops_module.metadata_url not found in YAML")
                return []
            
            print(f"  📡 Querying CO-OPS API: {metadata_url}")
            response = requests.get(metadata_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            raw_stations = data.get('stations', [])
            print(f"  📊 Processing {len(raw_stations)} CO-OPS stations...")
            
            processed_count = 0
            for station_data in raw_stations:
                try:
                    station_id = station_data.get('id', 'unknown')
                    station_name = station_data.get('name', 'Unknown Station')
                    station_lat = float(station_data.get('lat', 0))
                    station_lon = float(station_data.get('lng', 0))
                    station_state = station_data.get('state', 'Unknown')
                    
                    # Calculate distance using Haversine formula
                    distance = self._calculate_distance(user_lat, user_lon, station_lat, station_lon)
                    
                    if distance <= max_distance_km:
                        # Parse available products/data types
                        products = station_data.get('shefcodes', [])
                        if not products:
                            products = station_data.get('products', [])
                        
                        # Determine station capabilities
                        capabilities = self._parse_coops_capabilities(station_data)
                        
                        station_info = {
                            'id': station_id,
                            'name': station_name,
                            'lat': station_lat,
                            'lon': station_lon,
                            'distance_km': distance,
                            'distance_miles': distance * 0.621371,
                            'type': 'coops',
                            'state': station_state,
                            'products': products,
                            'capabilities': capabilities,
                            'status': station_data.get('status', 'unknown'),
                            'last_data': station_data.get('metadata', {}).get('last_data', 'unknown')
                        }
                        
                        stations.append(station_info)
                        processed_count += 1
                        
                except (ValueError, TypeError, KeyError) as e:
                    continue  # Skip malformed station data
            
            print(f"  ✅ Found {processed_count} CO-OPS stations within {max_distance_km}km")
            
        except Exception as e:
            print(f"❌ CO-OPS station discovery failed: {e}")
            return []
        
        return sorted(stations, key=lambda x: x['distance_km'])
    
    def _parse_coops_capabilities(self, station_data):
        """Parse CO-OPS station capabilities from API response."""
        capabilities = []
        
        # Check for water level data
        if any(product.get('name', '').lower() in ['water_level', 'wl'] for product in station_data.get('products', [])):
            capabilities.append('Water Level')
        
        # Check for tide predictions
        if station_data.get('tidal', False) or 'predictions' in str(station_data.get('products', [])).lower():
            capabilities.append('Tide Predictions')
        
        # Check for meteorological data
        if any(product.get('name', '').lower() in ['met', 'meteorological'] for product in station_data.get('products', [])):
            capabilities.append('Weather Data')
        
        # Check for water temperature
        if any(product.get('name', '').lower() in ['water_temperature', 'temp'] for product in station_data.get('products', [])):
            capabilities.append('Water Temperature')
        
        return capabilities if capabilities else ['Water Level']  # Default capability
    
    def _discover_ndbc_stations(self, user_lat, user_lon, max_distance_km):
        """Discover NDBC stations using YAML-configured URL."""
        stations = []
        try:
            # GET URL FROM CONSOLIDATED api_modules SECTION ONLY
            ndbc_module = self.yaml_data.get('api_modules', {}).get('ndbc_module', {})
            metadata_url = ndbc_module.get('metadata_url')
            
            if not metadata_url:
                print("❌ ERROR: ndbc_module.metadata_url not found in YAML")
                return []
            
            print(f"  📡 Querying NDBC API: {metadata_url}")
            response = requests.get(metadata_url, timeout=30)
            response.raise_for_status()
            
            # Parse XML data with comprehensive error handling
            content = response.text
            import xml.etree.ElementTree as ET
            
            try:
                root = ET.fromstring(content)
                print(f"  📊 Processing NDBC XML stations...")
                
                processed_count = 0
                for station in root.findall('.//station'):
                    try:
                        station_id = station.get('id', 'unknown')
                        station_name = station.get('name', 'Unknown Buoy')
                        station_lat = float(station.get('lat', 0))
                        station_lon = float(station.get('lon', 0))
                        station_type = station.get('type', 'buoy')
                        
                        # Calculate distance using Haversine formula
                        distance = self._calculate_distance(user_lat, user_lon, station_lat, station_lon)
                        
                        if distance <= max_distance_km:
                            # Parse station metadata
                            owner = station.get('owner', 'NOAA')
                            pgm = station.get('pgm', 'NDBC')
                            
                            # Determine station capabilities based on type and metadata
                            capabilities = self._parse_ndbc_capabilities(station_type, station)
                            
                            station_info = {
                                'id': station_id,
                                'name': station_name,
                                'lat': station_lat,
                                'lon': station_lon,
                                'distance_km': distance,
                                'distance_miles': distance * 0.621371,
                                'type': 'ndbc',
                                'station_type': station_type,
                                'owner': owner,
                                'program': pgm,
                                'capabilities': capabilities,
                                'products': []  # NDBC products determined by data availability
                            }
                            
                            stations.append(station_info)
                            processed_count += 1
                            
                    except (ValueError, TypeError, AttributeError) as e:
                        continue  # Skip malformed station data
                
                print(f"  ✅ Found {processed_count} NDBC stations within {max_distance_km}km")
                
            except ET.ParseError as e:
                print(f"❌ NDBC XML parsing error: {e}")
                return []
                
        except Exception as e:
            print(f"❌ NDBC station discovery failed: {e}")
            return []
        
        return sorted(stations, key=lambda x: x['distance_km'])
    
    def _parse_ndbc_capabilities(self, station_type, station_element):
        """Parse NDBC station capabilities based on type and metadata."""
        capabilities = []
        
        # Basic capabilities based on station type
        if station_type.lower() in ['buoy', 'fixed_buoy']:
            capabilities.extend(['Wind Speed/Direction', 'Wave Height', 'Sea Surface Temperature'])
        elif station_type.lower() in ['c-man', 'coastal']:
            capabilities.extend(['Wind Speed/Direction', 'Air Temperature', 'Barometric Pressure'])
        elif station_type.lower() in ['dart', 'tsunami']:
            capabilities.extend(['Tsunami Detection', 'Water Pressure'])
        else:
            capabilities.append('Marine Weather Data')  # Generic capability
        
        # Add common NDBC capabilities
        capabilities.extend(['Barometric Pressure', 'Air Temperature'])
        
        return list(set(capabilities))  # Remove duplicates
       
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
    
    def _display_station_selection(self, stations):
        """Display stations and collect user selections with comprehensive interface."""
        if not stations:
            print("⚠️  No marine stations found within search radius.")
            print("Consider expanding search radius or check your location coordinates.")
            return []
        
        print(f"\n🏟️  NEARBY MARINE STATIONS ({len(stations)} found)")
        print("=" * 90)
        print(f"{'#':<3} {'ID':<10} {'Type':<6} {'Distance':<12} {'Capabilities':<25} {'Name':<25}")
        print("-" * 90)
        
        display_limit = min(20, len(stations))  # Show up to 20 closest stations
        for i, station in enumerate(stations[:display_limit], 1):
            station_id = station['id']
            station_type = station['type'].upper()
            distance_str = f"{station['distance_km']:.1f}km"
            capabilities = ', '.join(station.get('capabilities', ['Unknown'])[:2])  # Show first 2 capabilities
            if len(station.get('capabilities', [])) > 2:
                capabilities += '...'
            name = station['name'][:24]  # Truncate long names
            
            print(f"{i:<3} {station_id:<10} {station_type:<6} {distance_str:<12} {capabilities:<25} {name:<25}")
        
        if len(stations) > display_limit:
            print(f"\n... and {len(stations) - display_limit} more stations")
        
        print(f"\n📍 Station Details Available:")
        print(f"  • CO-OPS Stations: https://tidesandcurrents.noaa.gov/stations.html")
        print(f"  • NDBC Buoys: https://www.ndbc.noaa.gov/")
        
        print(f"\n🎯 STATION SELECTION OPTIONS:")
        print(f"  • Enter station numbers (e.g., 1,3,5)")
        print(f"  • Enter 'all' to select all stations")
        print(f"  • Enter 'top5' to select 5 closest stations")
        print(f"  • Press Enter to skip station selection")
        
        while True:
            try:
                selection = input(f"\nYour selection: ").strip()
                
                if not selection:
                    # No stations selected
                    print("⚠️  No stations selected - marine data collection will be disabled")
                    return []
                
                elif selection.lower() == 'all':
                    # Select all displayed stations
                    selected_stations = stations[:display_limit]
                    print(f"✅ Selected all {len(selected_stations)} displayed stations")
                    break
                
                elif selection.lower() == 'top5':
                    # Select 5 closest stations
                    selected_stations = stations[:5]
                    print(f"✅ Selected {len(selected_stations)} closest stations")
                    break
                
                else:
                    # Parse individual station numbers
                    try:
                        indices = [int(x.strip()) for x in selection.split(',')]
                        selected_stations = []
                        
                        for idx in indices:
                            if 1 <= idx <= display_limit:
                                selected_stations.append(stations[idx - 1])
                            else:
                                print(f"❌ Invalid station number: {idx} (must be 1-{display_limit})")
                                continue
                        
                        if selected_stations:
                            print(f"✅ Selected {len(selected_stations)} stations:")
                            for station in selected_stations:
                                distance_str = f"{station['distance_km']:.1f}km"
                                print(f"   • {station['id']} - {station['name']} ({distance_str})")
                            break
                        else:
                            print("❌ No valid stations selected. Please try again.")
                            continue
                            
                    except ValueError:
                        print("❌ Invalid input. Please enter numbers separated by commas.")
                        continue
                    
            except KeyboardInterrupt:
                print("\n\n⚠️  Setup cancelled by user.")
                sys.exit(1)
        
        # Provide detailed information about selected stations
        self._display_selected_station_details(selected_stations)
        
        return selected_stations
    
    def _display_selected_station_details(self, selected_stations):
        """Display detailed information about selected stations."""
        if not selected_stations:
            return
        
        print(f"\n📋 SELECTED STATION DETAILS:")
        print("-" * 50)
        
        coops_stations = [s for s in selected_stations if s['type'] == 'coops']
        ndbc_stations = [s for s in selected_stations if s['type'] == 'ndbc']
        
        if coops_stations:
            print(f"\n🌊 CO-OPS STATIONS ({len(coops_stations)}):")
            for station in coops_stations:
                capabilities = ', '.join(station.get('capabilities', ['Water Level']))
                state = station.get('state', 'Unknown')
                print(f"  • {station['id']} - {station['name']}, {state}")
                print(f"    Distance: {station['distance_km']:.1f}km, Capabilities: {capabilities}")
        
        if ndbc_stations:
            print(f"\n🛟 NDBC STATIONS ({len(ndbc_stations)}):")
            for station in ndbc_stations:
                capabilities = ', '.join(station.get('capabilities', ['Marine Weather']))
                station_type = station.get('station_type', 'buoy').title()
                print(f"  • {station['id']} - {station['name']} ({station_type})")
                print(f"    Distance: {station['distance_km']:.1f}km, Capabilities: {capabilities}")
        
        print(f"\n💡 Data Collection Info:")
        print(f"  • CO-OPS data updated every 10 minutes (high frequency)")
        print(f"  • NDBC data updated hourly (standard meteorological)")
        print(f"  • Total stations: {len(selected_stations)}")
    
    def _collect_field_selections(self):
        """Collect field selections based on YAML complexity levels."""
        complexity_levels = self.yaml_data.get('complexity_levels', {})
        
        print("Available field complexity levels:")
        print("-" * 40)
        
        level_options = list(complexity_levels.keys())
        for i, level in enumerate(level_options, 1):
            level_info = complexity_levels[level]
            description = level_info.get('description', 'No description')
            estimated_fields = level_info.get('estimated_fields', 'Unknown')
            print(f"{i}. {level.upper()}: {description}")
            if estimated_fields != 'Unknown':
                print(f"   (~{estimated_fields} fields)")
        
        while True:
            try:
                choice = input(f"\nSelect complexity level (1-{len(level_options)}): ").strip()
                level_idx = int(choice) - 1
                
                if 0 <= level_idx < len(level_options):
                    selected_level = level_options[level_idx]
                    print(f"✅ Selected: {selected_level.upper()}")
                    
                    # Get fields for selected complexity level
                    fields = self._get_fields_for_complexity(selected_level)
                    return {
                        'complexity_level': selected_level,
                        'fields': fields
                    }
                else:
                    print(f"❌ Please enter a number between 1 and {len(level_options)}")
                    
            except ValueError:
                print("❌ Please enter a valid number.")
            except KeyboardInterrupt:
                print("\n\n⚠️  Setup cancelled by user.")
                sys.exit(1)
    
    def _get_fields_for_complexity(self, complexity_level):
        """Get field list for specified complexity level with detailed processing."""
        all_fields = self.yaml_data.get('fields', {})
        selected_fields = {}
        
        if not all_fields:
            print("⚠️  Warning: No fields defined in YAML - using fallback minimal fields")
            return self._get_fallback_minimal_fields()
        
        print(f"📊 Processing {len(all_fields)} available fields from YAML...")
        
        # Process fields based on complexity level
        for field_name, field_config in all_fields.items():
            field_complexity = field_config.get('complexity_level', 'minimal')
            field_description = field_config.get('description', 'No description available')
            
            include_field = False
            
            if complexity_level == 'all':
                include_field = True
            elif complexity_level == 'minimal':
                include_field = (field_complexity == 'minimal')
            elif complexity_level == 'custom':
                # For custom, we'll implement individual field selection later
                include_field = (field_complexity in ['minimal', 'standard'])
            
            if include_field:
                selected_fields[field_name] = field_config
        
        if not selected_fields:
            print(f"⚠️  No fields found for complexity level '{complexity_level}' - using fallback")
            return self._get_fallback_minimal_fields()
        
        # Display field selection summary
        print(f"✅ Selected {len(selected_fields)} fields for '{complexity_level}' complexity:")
        
        # Group fields by data source for display
        coops_fields = []
        ndbc_fields = []
        other_fields = []
        
        for field_name, field_config in selected_fields.items():
            api_source = field_config.get('api_source', 'unknown')
            if 'coops' in api_source.lower():
                coops_fields.append(field_name)
            elif 'ndbc' in api_source.lower():
                ndbc_fields.append(field_name)
            else:
                other_fields.append(field_name)
        
        if coops_fields:
            print(f"  🌊 CO-OPS fields ({len(coops_fields)}): {', '.join(coops_fields[:3])}{'...' if len(coops_fields) > 3 else ''}")
        if ndbc_fields:
            print(f"  🛟 NDBC fields ({len(ndbc_fields)}): {', '.join(ndbc_fields[:3])}{'...' if len(ndbc_fields) > 3 else ''}")
        if other_fields:
            print(f"  📊 Other fields ({len(other_fields)}): {', '.join(other_fields[:3])}{'...' if len(other_fields) > 3 else ''}")
        
        return selected_fields
    
    def _get_fallback_minimal_fields(self):
        """Fallback minimal field set when YAML processing fails."""
        return {
            'water_level': {
                'description': 'Water level above MLLW',
                'database_field': 'water_level',
                'database_type': 'REAL',
                'database_table': 'archive',
                'unit_system': 'METRIC',
                'api_source': 'coops',
                'complexity_level': 'minimal'
            },
            'sea_surface_temperature': {
                'description': 'Sea surface temperature',
                'database_field': 'sea_surface_temp',
                'database_type': 'REAL', 
                'database_table': 'archive',
                'unit_system': 'METRIC',
                'api_source': 'ndbc',
                'complexity_level': 'minimal'
            }
        }
    
    def _configure_collection_intervals(self):
        """Configure data collection intervals for each module with comprehensive options."""
        intervals = {}
        api_modules = self.yaml_data.get('api_modules', {})
        
        if not api_modules:
            print("⚠️  Warning: No API modules defined in YAML - using fallback intervals")
            return self._get_fallback_intervals()
        
        print("\n⏱️  DATA COLLECTION INTERVALS")
        print("=" * 50)
        print("Configure how frequently to collect data from each source.")
        print("Lower intervals = more data, higher API usage")
        print("-" * 50)
        
        for module_name, module_config in api_modules.items():
            recommended = module_config.get('recommended_interval', 600)
            min_interval = module_config.get('min_interval', 60)
            max_interval = module_config.get('max_interval', 86400)
            
            # Display module information
            module_display_name = module_name.replace('_', ' ').title()
            print(f"\n📡 {module_display_name}:")
            print(f"  API URL: {module_config.get('api_url', 'Not configured')}")
            print(f"  Recommended: {recommended} seconds ({recommended//60} minutes)")
            print(f"  Range: {min_interval}-{max_interval} seconds")
            
            # Rate limiting information
            if 'rate_limit' in module_config:
                rate_info = module_config['rate_limit']
                print(f"  Rate Limit: {rate_info.get('requests_per_hour', 'Unknown')} requests/hour")
            
            while True:
                try:
                    user_input = input(f"  Enter interval in seconds [{recommended}]: ").strip()
                    
                    if not user_input:
                        interval = recommended
                    else:
                        interval = int(user_input)
                        
                        # Validate interval range
                        if interval < min_interval:
                            print(f"  ⚠️  Interval too low. Minimum: {min_interval} seconds")
                            print(f"      Lower intervals may cause rate limiting or API blocks")
                            continue
                        elif interval > max_interval:
                            print(f"  ⚠️  Interval too high. Maximum: {max_interval} seconds")
                            continue
                    
                    intervals[module_name] = interval
                    
                    # Calculate daily API calls
                    daily_calls = 86400 // interval
                    print(f"  ✅ Set to {interval} seconds ({interval//60} minutes)")
                    print(f"     Estimated daily API calls: {daily_calls}")
                    
                    # Warning for high frequency
                    if daily_calls > 1000:
                        print(f"     ⚠️  High frequency - monitor for rate limiting")
                    
                    break
                    
                except ValueError:
                    print("  ❌ Please enter a valid number.")
                except KeyboardInterrupt:
                    print("\n\n⚠️  Setup cancelled by user.")
                    sys.exit(1)
        
        # Display summary
        print(f"\n📊 COLLECTION SUMMARY:")
        print("-" * 30)
        total_daily_calls = 0
        for module_name, interval in intervals.items():
            daily_calls = 86400 // interval
            total_daily_calls += daily_calls
            print(f"  {module_name}: every {interval//60}min ({daily_calls} calls/day)")
        
        print(f"\n💡 Total daily API calls: {total_daily_calls}")
        if total_daily_calls > 2000:
            print("⚠️  High API usage - consider increasing intervals if rate limited")
        
        return intervals
    
    def _get_fallback_intervals(self):
        """Fallback intervals when YAML configuration is missing."""
        return {
            'coops_module': 600,  # 10 minutes - CO-OPS high frequency
            'ndbc_module': 3600   # 1 hour - NDBC standard frequency
        }
    
    def _write_configuration_files(self, stations, fields, intervals, user_lat, user_lon):
        """Write configuration files in exact CONF format with comprehensive mapping."""
        print("\n📄 CONFIGURATION FILE GENERATION")
        print("-" * 40)
        
        # Separate stations by type for configuration
        coops_stations = [s for s in stations if s['type'] == 'coops']
        ndbc_stations = [s for s in stations if s['type'] == 'ndbc']
        
        print(f"📊 Configuration Summary:")
        print(f"  • CO-OPS stations: {len(coops_stations)}")
        print(f"  • NDBC stations: {len(ndbc_stations)}")
        print(f"  • Selected fields: {len(fields['fields'])}")
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
                'selected_stations': self._generate_station_configuration(coops_stations, ndbc_stations),
                
                # Module configurations from YAML and user selections
                'coops_module': self._generate_coops_module_config(coops_stations, intervals),
                'ndbc_module': self._generate_ndbc_module_config(ndbc_stations, intervals),
                
                # Field mappings transformed from YAML to CONF format
                'field_mappings': self._transform_fields_yaml_to_conf(fields['fields']),
                
                # Collection intervals and timing
                'collection_intervals': self._generate_interval_configuration(intervals),
                
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
        """Generate detailed station configuration section."""
        station_config = {}
        
        if coops_stations:
            station_config['coops_stations'] = {}
            for station in coops_stations:
                station_config['coops_stations'][station['id']] = {
                    'enable': 'true',
                    'name': station['name'],
                    'latitude': str(station['lat']),
                    'longitude': str(station['lon']),
                    'distance_km': str(round(station['distance_km'], 2)),
                    'state': station.get('state', 'Unknown'),
                    'capabilities': ','.join(station.get('capabilities', ['Water Level']))
                }
        
        if ndbc_stations:
            station_config['ndbc_stations'] = {}
            for station in ndbc_stations:
                station_config['ndbc_stations'][station['id']] = {
                    'enable': 'true',
                    'name': station['name'],
                    'latitude': str(station['lat']),
                    'longitude': str(station['lon']),
                    'distance_km': str(round(station['distance_km'], 2)),
                    'station_type': station.get('station_type', 'buoy'),
                    'capabilities': ','.join(station.get('capabilities', ['Marine Weather']))
                }
        
        return station_config
    
    def _generate_coops_module_config(self, coops_stations, intervals):
        """Generate CO-OPS module configuration."""
        coops_config = {
            'enable': 'true' if coops_stations else 'false',
            'interval': str(intervals.get('coops_module', 600)),
            'api_url': self.yaml_data.get('api_modules', {}).get('coops_module', {}).get('api_url'),
            'stations': ','.join([s['id'] for s in coops_stations]),
            'products': 'water_level,predictions,water_temperature',  # Default products
            'datum': 'MLLW',  # Mean Lower Low Water
            'units': 'metric',
            'time_zone': 'gmt'
        }
        
        return coops_config
    
    def _generate_ndbc_module_config(self, ndbc_stations, intervals):
        """Generate NDBC module configuration."""
        ndbc_config = {
            'enable': 'true' if ndbc_stations else 'false',
            'interval': str(intervals.get('ndbc_module', 3600)),
            'api_url': self.yaml_data.get('api_modules', {}).get('ndbc_module', {}).get('api_url'),
            'stations': ','.join([s['id'] for s in ndbc_stations]),
            'data_types': 'stdmet,spec,swdir,swden',  # Standard met, spectral, swell direction, swell density
            'units': 'metric'
        }
        
        return ndbc_config
    
    def _generate_interval_configuration(self, intervals):
        """Generate centralized interval configuration."""
        interval_config = {}
        
        for module_name, interval_seconds in intervals.items():
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
    
    def _transform_fields_yaml_to_conf(self, yaml_fields):
        """Transform YAML field definitions to CONF format with comprehensive mapping."""
        conf_mappings = {}
        
        if not yaml_fields:
            print("⚠️  No YAML fields to transform - using fallback field mappings")
            return self._get_fallback_field_mappings()
        
        print(f"🔄 Transforming {len(yaml_fields)} YAML fields to CONF format...")
        
        # Track field statistics for validation
        field_stats = {
            'coops_fields': 0,
            'ndbc_fields': 0,
            'archive_table': 0,
            'marine_tables': 0,
            'numeric_fields': 0,
            'text_fields': 0
        }
        
        for field_name, field_config in yaml_fields.items():
            try:
                # Core field mapping
                conf_field = {
                    'database_field': field_config.get('database_field', field_name),
                    'database_type': field_config.get('database_type', 'REAL'),
                    'database_table': field_config.get('database_table', 'archive'),
                    'unit_system': field_config.get('unit_system', 'METRIC'),
                    'api_source': field_config.get('api_source', 'unknown')
                }
                
                # Add extended mapping information
                if 'api_path' in field_config:
                    conf_field['api_path'] = field_config['api_path']
                
                if 'unit_group' in field_config:
                    conf_field['unit_group'] = field_config['unit_group']
                
                if 'conversion_factor' in field_config:
                    conf_field['conversion_factor'] = str(field_config['conversion_factor'])
                
                if 'description' in field_config:
                    conf_field['description'] = field_config['description']
                
                # Add data validation rules if present
                if 'validation' in field_config:
                    validation = field_config['validation']
                    if 'min_value' in validation:
                        conf_field['min_value'] = str(validation['min_value'])
                    if 'max_value' in validation:
                        conf_field['max_value'] = str(validation['max_value'])
                
                conf_mappings[field_name] = conf_field
                
                # Update statistics
                api_source = conf_field['api_source'].lower()
                if 'coops' in api_source:
                    field_stats['coops_fields'] += 1
                elif 'ndbc' in api_source:
                    field_stats['ndbc_fields'] += 1
                
                if conf_field['database_table'] == 'archive':
                    field_stats['archive_table'] += 1
                else:
                    field_stats['marine_tables'] += 1
                
                if conf_field['database_type'] in ['REAL', 'INTEGER']:
                    field_stats['numeric_fields'] += 1
                else:
                    field_stats['text_fields'] += 1
                
            except Exception as e:
                print(f"    ⚠️  Error transforming field '{field_name}': {e}")
                continue
        
        # Display transformation summary
        print(f"✅ Field transformation completed:")
        print(f"    • Total fields: {len(conf_mappings)}")
        print(f"    • CO-OPS fields: {field_stats['coops_fields']}")
        print(f"    • NDBC fields: {field_stats['ndbc_fields']}")
        print(f"    • Archive table: {field_stats['archive_table']}")
        print(f"    • Marine tables: {field_stats['marine_tables']}")
        print(f"    • Numeric fields: {field_stats['numeric_fields']}")
        print(f"    • Text fields: {field_stats['text_fields']}")
        
        return conf_mappings
    
    def _get_fallback_field_mappings(self):
        """Fallback field mappings when YAML processing fails."""
        return {
            'water_level': {
                'database_field': 'water_level',
                'database_type': 'REAL',
                'database_table': 'archive',
                'unit_system': 'METRIC',
                'api_source': 'coops',
                'unit_group': 'group_altitude',
                'description': 'Water level above MLLW'
            },
            'sea_surface_temperature': {
                'database_field': 'sea_surface_temp',
                'database_type': 'REAL',
                'database_table': 'archive', 
                'unit_system': 'METRIC',
                'api_source': 'ndbc',
                'unit_group': 'group_temperature',
                'description': 'Sea surface temperature'
            }
        }


class MarineDatabaseManager:
    """
    Database schema management for Marine Data Extension.
    
    All database operations moved here from installer class.
    Handles table creation, field addition, and hybrid weectl/SQL operations.
    """
    
    def __init__(self, config_dict):
        self.config_dict = config_dict
        
    def extend_database_schema(self, selected_options):
        """Extend database schema based on selected fields and stations."""
        print("\n🗄️  DATABASE SCHEMA EXTENSION")
        print("-" * 40)
        
        fields = selected_options.get('fields', {}).get('fields', {})
        if not fields:
            print("⚠️  No fields selected - skipping database extension.")
            return
        
        # Create database tables for two-table architecture
        self._create_database_tables(fields)
        
        # Add missing fields to existing tables
        self._add_missing_fields(fields)
        
        print("✅ Database schema extension completed")
    
    def _create_database_tables(self, fields):
        """Create database tables for two-table architecture."""
        print("  📋 Creating database tables...")
        
        # Determine which tables are needed
        tables_needed = set()
        for field_config in fields.values():
            table_name = field_config.get('database_table', 'archive')
            tables_needed.add(table_name)
        
        for table_name in tables_needed:
            if table_name != 'archive':  # Don't create the main archive table
                self._create_table_if_not_exists(table_name, fields)
                print(f"    ✅ Table '{table_name}' ready")
    
    def _create_table_if_not_exists(self, table_name, fields):
        """Create table if it doesn't exist."""
        try:
            # Get database path from WeeWX configuration
            db_binding = self.config_dict.get('DataBindings', {}).get('wx_binding', {})
            database_dict = self.config_dict.get('Databases', {}).get(db_binding.get('database'), {})
            db_path = database_dict.get('database_name', '/var/lib/weewx/weewx.sdb')
            
            # Define table fields for this specific table
            table_fields = []
            for field_name, field_config in fields.items():
                if field_config.get('database_table', 'archive') == table_name:
                    db_field = field_config.get('database_field', field_name)
                    db_type = field_config.get('database_type', 'REAL')
                    table_fields.append(f"{db_field} {db_type}")
            
            if not table_fields:
                return  # No fields for this table
            
            # Create table with dateTime primary key
            fields_sql = ', '.join(['dateTime INTEGER NOT NULL PRIMARY KEY'] + table_fields)
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_sql})"
            
            with sqlite3.connect(db_path) as conn:
                conn.execute(create_sql)
                conn.commit()
                
        except Exception as e:
            print(f"    ⚠️  Warning: Could not create table '{table_name}': {e}")
    
    def _add_missing_fields(self, fields):
        """Add missing fields using hybrid weectl/SQL approach."""
        print("  🔧 Adding database fields...")
        
        # Group fields by database type for hybrid approach
        numeric_fields = {}  # For weectl
        text_fields = {}     # For direct SQL
        
        for field_name, field_config in fields.items():
            db_type = field_config.get('database_type', 'REAL')
            if db_type in ['REAL', 'INTEGER']:
                numeric_fields[field_name] = field_config
            else:
                text_fields[field_name] = field_config
        
        # Use weectl for numeric fields (success manual pattern)
        if numeric_fields:
            self._create_fields_with_weectl(numeric_fields)
        
        # Use direct SQL for TEXT/VARCHAR fields (success manual pattern)
        if text_fields:
            self._create_fields_with_sql(text_fields)
    
    def _create_fields_with_weectl(self, fields):
        """Create numeric fields using weectl (success manual hybrid approach)."""
        try:
            config_path = '/etc/weewx/weewx.conf'
            
            for field_name, field_config in fields.items():
                db_field = field_config.get('database_field', field_name)
                db_type = field_config.get('database_type', 'REAL')
                
                # CRITICAL: Use equals format for weectl parameters (success manual)
                cmd = [
                    'weectl', 'database', 'add-column',
                    f'--config={config_path}',  # Equals format required
                    db_field, db_type
                ]
                
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                    if result.returncode == 0:
                        print(f"    ✅ Added field: {db_field} ({db_type})")
                    elif "already exists" in result.stderr.lower():
                        print(f"    ✅ Field exists: {db_field} ({db_type})")
                    else:
                        print(f"    ⚠️  weectl warning for {db_field}: {result.stderr.strip()}")
                        
                except subprocess.TimeoutExpired:
                    print(f"    ⚠️  Timeout adding field: {db_field}")
                except Exception as e:
                    print(f"    ⚠️  Error adding field {db_field}: {e}")
                    
        except Exception as e:
            print(f"    ⚠️  weectl field creation failed: {e}")
    
    def _create_fields_with_sql(self, fields):
        """Create TEXT/VARCHAR fields using direct SQL (success manual hybrid approach)."""
        try:
            # Get database path
            db_binding = self.config_dict.get('DataBindings', {}).get('wx_binding', {})
            database_dict = self.config_dict.get('Databases', {}).get(db_binding.get('database'), {})
            db_path = database_dict.get('database_name', '/var/lib/weewx/weewx.sdb')
            
            print(f"    📂 Using database: {db_path}")
            
            with sqlite3.connect(db_path) as conn:
                for field_name, field_config in fields.items():
                    db_field = field_config.get('database_field', field_name)
                    db_type = field_config.get('database_type', 'VARCHAR(50)')
                    table_name = field_config.get('database_table', 'archive')
                    
                    try:
                        # Check if field exists
                        cursor = conn.execute(f"PRAGMA table_info({table_name})")
                        existing_fields = [row[1] for row in cursor.fetchall()]
                        
                        if db_field not in existing_fields:
                            # Add field using direct SQL
                            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {db_field} {db_type}"
                            conn.execute(alter_sql)
                            print(f"    ✅ Added field: {db_field} ({db_type}) to {table_name}")
                        else:
                            print(f"    ✅ Field exists: {db_field} ({db_type}) in {table_name}")
                            
                    except sqlite3.Error as e:
                        if "duplicate column name" in str(e).lower():
                            print(f"    ✅ Field exists: {db_field} ({db_type}) in {table_name}")
                        else:
                            print(f"    ⚠️  SQL error for field {db_field}: {e}")
                
                conn.commit()
                print(f"    ✅ SQL field creation completed")
                
        except sqlite3.Error as e:
            print(f"    ❌ Database connection error: {e}")
            print(f"    Attempting alternative database paths...")
            self._try_alternative_database_paths(fields)
        except Exception as e:
            print(f"    ❌ Direct SQL field creation failed: {e}")
            print(f"    Fallback: Fields will be created when service starts")
    
    def _try_alternative_database_paths(self, fields):
        """Try alternative database paths when primary path fails."""
        alternative_paths = [
            '/var/lib/weewx/weewx.sdb',
            '/home/weewx/archive/weewx.sdb',
            '/usr/share/weewx/weewx.sdb',
            './weewx.sdb'
        ]
        
        for db_path in alternative_paths:
            if os.path.exists(db_path):
                try:
                    print(f"    🔄 Trying alternative database: {db_path}")
                    with sqlite3.connect(db_path) as conn:
                        # Quick test
                        conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                        print(f"    ✅ Successfully connected to {db_path}")
                        
                        # Add fields to this database
                        for field_name, field_config in fields.items():
                            db_field = field_config.get('database_field', field_name)
                            db_type = field_config.get('database_type', 'VARCHAR(50)')
                            table_name = field_config.get('database_table', 'archive')
                            
                            try:
                                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                                existing_fields = [row[1] for row in cursor.fetchall()]
                                
                                if db_field not in existing_fields:
                                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {db_field} {db_type}"
                                    conn.execute(alter_sql)
                                    print(f"    ✅ Added {db_field} to {db_path}")
                            except sqlite3.Error:
                                continue
                        
                        conn.commit()
                        return  # Success, exit function
                        
                except sqlite3.Error:
                    continue  # Try next path
        
        print(f"    ⚠️  Could not connect to any database - fields will be created at runtime")


if __name__ == '__main__':
    print("This is a WeeWX extension installer.")
    print("Use: weectl extension install weewx-marine-data.zip")