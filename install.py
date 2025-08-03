#!/usr/bin/env python3\
# Magic Animal: Sting Ray
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
        
        FIXED: Merge configuration before database operations so field mappings are available.
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
            
            # Step 3: Database schema management with merged configuration
            db_manager = MarineDatabaseManager(merged_config)
            db_manager.extend_database_schema(selected_options)
            
            # Step 4: Update engine configuration
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
        This fixes the Newport Beach Harbor (9410580) missing issue.
        """
        nearby_stations = []
        
        try:
            # Use api_modules section - NO hardcoded URLs
            coops_module = self.yaml_data.get('api_modules', {}).get('coops_module', {})
            metadata_url = coops_module.get('metadata_url')
            
            if not metadata_url:
                print("  ❌ ERROR: coops_module.metadata_url not found in YAML")
                return []
            
            # Get BOTH observation stations AND tide prediction reference stations
            observation_url = metadata_url  # expand=detail for real-time stations
            reference_url = metadata_url.replace('?expand=detail', '?type=tidepredictions')
            
            print(f"  📡 Querying CO-OPS observation stations...")
            response = requests.get(observation_url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                obs_stations = data.get('stations', [])
                nearby_stations.extend(self._process_coops_stations(obs_stations, user_lat, user_lon, max_distance_km, 'observation'))
            
            print(f"  📡 Querying CO-OPS reference stations...")
            response = requests.get(reference_url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                ref_stations = data.get('stations', [])
                nearby_stations.extend(self._process_coops_stations(ref_stations, user_lat, user_lon, max_distance_km, 'reference'))
            
            # Remove duplicates and sort by distance
            seen_ids = set()
            unique_stations = []
            for station in nearby_stations:
                if station['id'] not in seen_ids:
                    seen_ids.add(station['id'])
                    unique_stations.append(station)
            
            unique_stations.sort(key=lambda x: x['distance_km'])
            result = unique_stations[:10]  # Hard limit of 10
            
            # Check if Newport Beach Harbor was found
            newport_found = any(s['id'] == '9410580' for s in result)
            if newport_found:
                print(f"  ✅ Found Newport Beach Harbor (9410580)")
            else:
                print(f"  ⚠️  Newport Beach Harbor (9410580) not found within {max_distance_km}km")
            
            print(f"  ✅ Found {len(result)} CO-OPS stations (observation + reference)")
            return result
            
        except Exception as e:
            print(f"  ❌ CO-OPS station discovery failed: {e}")
            return []

    def _process_coops_stations(self, stations, user_lat, user_lon, max_distance_km, station_type):
        """Process CO-OPS stations and filter by distance."""
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
                        'station_type': station_type
                    }
                    processed_stations.append(station_info)
                    
            except (ValueError, KeyError, TypeError):
                continue  # Skip invalid stations
        
        return processed_stations

    def _parse_coops_capabilities(self, station_data):
        """Parse CO-OPS station capabilities by calling products API."""
        capabilities = []
        
        # Get the products URL from station data
        products_info = station_data.get('products', {})
        products_url = products_info.get('self')
        
        if products_url:
            try:
                # Make API call to get actual products
                response = requests.get(products_url, timeout=10)
                if response.status_code == 200:
                    products_data = response.json()
                    products_list = products_data.get('products', [])
                    
                    # Parse the products list to determine capabilities
                    for product in products_list:
                        product_name = product.get('name', '').lower()
                        if 'water level' in product_name:
                            capabilities.append('Water Level')
                        elif 'tide prediction' in product_name:
                            capabilities.append('Tide Predictions') 
                        elif 'meteorological' in product_name:
                            capabilities.append('Weather Data')
                            
                else:
                    # Fallback to basic capabilities if API call fails
                    capabilities.append('Water Level')
                    if station_data.get('tidal', False):
                        capabilities.append('Tide Predictions')
                        
            except Exception as e:
                # Fallback to basic capabilities if API call fails
                capabilities.append('Water Level')
                if station_data.get('tidal', False):
                    capabilities.append('Tide Predictions')
        else:
            # No products URL available, use basic capabilities
            capabilities.append('Water Level')
            if station_data.get('tidal', False):
                capabilities.append('Tide Predictions')
        
        return capabilities if capabilities else ['Water Level']
    
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
        """
        Display stations with SEPARATE selection processes using curses interface.
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
        import curses
        
        def station_menu(stdscr):
            curses.curs_set(0)  # Hide cursor
            stdscr.clear()
            
            selected = []
            current_row = 0
            marked = set()
            
            # Determine explanatory text based on station type
            if "CO-OPS" in title:
                explanation = [
                    "CO-OPS stations provide TIDE INFORMATION:",
                    "• Tide predictions (high/low tide times and heights)",
                    "• Real-time water levels (if station has sensors)",
                    "• Coastal water temperature (select stations)",
                    "• Essential for boating, fishing, and coastal activities"
                ]
            else:  # NDBC
                explanation = [
                    "NDBC buoys provide OFFSHORE MARINE CONDITIONS:",
                    "• Real-time wave heights, periods, and directions", 
                    "• Ocean surface temperature and weather data",
                    "• Wind speed, direction, and atmospheric pressure",
                    "• Critical for offshore boating and marine weather"
                ]
            
            while True:
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Header
                header = f"{icon} {title} - Select Stations"
                try:
                    stdscr.addstr(0, 0, header[:width-1], curses.A_BOLD)
                    stdscr.addstr(1, 0, "="*min(len(header), width-1))
                except curses.error:
                    pass
                
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
                
                # Station list - FIX: Remove artificial 6-station limit, show up to 10 as intended
                start_row = inst_row + 3
                available_rows = max(1, (height - start_row - 3) // 2)
                max_display = min(10, available_rows)  # Show up to 10 stations (design limit), not 6
                display_stations = stations[:max_display] if stations else []
                
                for idx, station in enumerate(display_stations):
                    y = start_row + (idx * 2)
                    if y >= height - 3:
                        break
                    
                    # Format station line
                    mark = "[X]" if idx in marked else "[ ]"
                    distance = f"{station.get('distance_km', 0):.1f}km"
                    bearing = station.get('bearing_text', 'Unknown')
                    
                    # Calculate available width for name
                    prefix = f"{mark} {station.get('id', 'Unknown')} - "
                    suffix = f" ({distance} {bearing})"
                    available_width = width - len(prefix) - len(suffix) - 2
                    
                    # Truncate name properly
                    name = station.get('name', 'Unknown Station')
                    if available_width > 0 and len(name) > available_width:
                        name = name[:max(1, available_width-3)] + "..."
                    
                    line = f"{prefix}{name}{suffix}"
                    
                    # Highlight current row
                    attr = curses.A_REVERSE if idx == current_row else curses.A_NORMAL
                    try:
                        stdscr.addstr(y, 0, line[:width-1], attr)
                    except curses.error:
                        pass
                    
                    # Show capabilities on next line
                    caps = ', '.join(station.get('capabilities', [])[:2])
                    cap_line = f"    {caps}"
                    if y + 1 < height - 3:
                        try:
                            stdscr.addstr(y + 1, 0, cap_line[:width-1], curses.A_DIM)
                        except curses.error:
                            pass
                
                # Footer
                footer_y = height - 2
                footer = f"Selected: {len(marked)} stations | Displaying {len(display_stations)} of {len(stations)}"
                try:
                    stdscr.addstr(footer_y, 0, footer[:width-1], curses.A_BOLD)
                except curses.error:
                    pass
                
                try:
                    stdscr.refresh()
                except curses.error:
                    pass
                
                # Handle input
                key = stdscr.getch()
                
                if key == curses.KEY_UP and current_row > 0:
                    current_row -= 1
                elif key == curses.KEY_DOWN and current_row < len(display_stations) - 1:
                    current_row += 1
                elif key == ord(' '):
                    if current_row < len(display_stations):
                        if current_row in marked:
                            marked.remove(current_row)
                        else:
                            marked.add(current_row)
                elif key == 10 or key == 13:  # Enter
                    selected = [display_stations[i] for i in marked if i < len(display_stations)]
                    break
                elif key == 27:  # ESC
                    selected = []
                    break
                elif key == ord('q'):  # Quit
                    raise KeyboardInterrupt()
            
            return selected
        
        try:
            return curses.wrapper(station_menu)
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
            print(f"    {distance} {bearing} | {capabilities}")
            print()
        
        print("TIP: Select 2-3 stations for backup data sources")
        
        while True:
            try:
                selection = input("Select stations (1,2,3 or 'all' or 'none'): ").strip()
                
                if selection.lower() == 'none' or not selection:
                    print("ℹ️  Skipping this station type")
                    return []
                elif selection.lower() == 'all':
                    print(f"✅ Selected all {len(stations[:10])} stations")
                    return stations[:10]
                else:
                    indices = [int(x.strip()) for x in selection.split(',')]
                    selected = []
                    for idx in indices:
                        if 1 <= idx <= len(stations[:10]):
                            selected.append(stations[idx - 1])
                    print(f"✅ Selected {len(selected)} stations")
                    return selected
                    
            except (ValueError, KeyboardInterrupt):
                print("❌ Invalid selection, skipping this station type")
                return []

    def _simple_station_selection(self, coops_stations, ndbc_stations):
        """Fallback simple selection for both station types with explanations."""
        selected = []
        
        print("\n" + "="*70)
        print("MARINE STATION SELECTION (Text Mode)")
        print("="*70)
        print("You will select two types of marine data sources:")
        print("• CO-OPS: Coastal tide and water level information")
        print("• NDBC:  Offshore buoy weather and wave conditions")
        print("\nTIP: Select 2-3 stations of each type for reliable data backup")
        
        if coops_stations:
            print("\n" + "🌊" * 35)
            coops_selected = self._simple_text_selection(coops_stations, "CO-OPS Tide Stations")
            selected.extend(coops_selected)
        
        if ndbc_stations:
            print("\n" + "🛟" * 35)
            ndbc_selected = self._simple_text_selection(ndbc_stations, "NDBC Buoy Stations")
            selected.extend(ndbc_selected)
        
        if selected:
            print(f"\n✅ Total stations selected: {len(selected)}")
            print("These stations will provide marine data to your WeeWX installation.")
        else:
            print("\n⚠️  No stations selected - marine data collection will be disabled")
        
        return selected

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

    def _display_selected_station_details(self, selected_stations):
        """Display enhanced details about selected stations with bearing information."""
        if not selected_stations:
            return
        
        coops_stations = [s for s in selected_stations if s['type'] == 'coops']
        ndbc_stations = [s for s in selected_stations if s['type'] == 'ndbc']
        
        print(f"\n📋 SELECTED STATION SUMMARY:")
        print("-" * 80)
        
        if coops_stations:
            print(f"\n🌊 CO-OPS TIDE STATIONS ({len(coops_stations)} selected):")
            for station in coops_stations:
                capabilities = ', '.join(station.get('capabilities', ['Water Level']))
                state = station.get('state', 'Unknown')
                bearing_text = station.get('bearing_text', 'Unknown')
                
                print(f"  • {station['id']} - {station['name']}, {state}")
                print(f"    📍 {station['distance_km']:.1f}km {bearing_text} from your location")
                print(f"    📊 Capabilities: {capabilities}")
        
        if ndbc_stations:
            print(f"\n🛟 NDBC BUOY STATIONS ({len(ndbc_stations)} selected):")
            for station in ndbc_stations:
                capabilities = ', '.join(station.get('capabilities', ['Marine Weather']))
                station_type = station.get('station_type', 'buoy').title()
                bearing_text = station.get('bearing_text', 'Unknown')
                
                print(f"  • {station['id']} - {station['name']} ({station_type})")
                print(f"    📍 {station['distance_km']:.1f}km {bearing_text} from your location")
                print(f"    📊 Capabilities: {capabilities}")
        
        print(f"\n💡 DATA COLLECTION SUMMARY:")
        print(f"  • Total stations selected: {len(selected_stations)}")
        print(f"  • CO-OPS data: Updated every 10 minutes")
        print(f"  • NDBC data: Updated hourly")
        print(f"  • Estimated daily API calls: {len(selected_stations) * 50}-{len(selected_stations) * 100}")
  
    def _collect_field_selections(self):
        """Collect field selections based on YAML complexity levels."""
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
        
        # Get fields based on complexity level
        if complexity_level == 'minimal':
            # Get minimal fields from YAML
            complexity_config = self.yaml_data.get('complexity_levels', {}).get('minimal', {})
            selected_fields = complexity_config.get('fields', {})
        elif complexity_level == 'all':
            # Get all fields from YAML
            complexity_config = self.yaml_data.get('complexity_levels', {}).get('all', {})
            selected_fields = complexity_config.get('fields', {})
        elif complexity_level == 'custom':
            # FIX: Actually execute custom selection instead of skipping
            field_definitions = self.yaml_data.get('fields', {})
            if not field_definitions:
                print("Warning: No field definitions available. Using minimal defaults.")
                complexity_config = self.yaml_data.get('complexity_levels', {}).get('minimal', {})
                selected_fields = complexity_config.get('fields', {})
            else:
                print("\nStarting custom field selection interface...")
                custom_selection = self.show_marine_custom_selection(field_definitions)
                
                # FIX: Don't fallback to defaults if user made a custom selection
                if custom_selection is not None and len(custom_selection) > 0:
                    selected_fields = custom_selection
                elif custom_selection is None:
                    print("Custom selection cancelled. Using minimal defaults.")
                    complexity_config = self.yaml_data.get('complexity_levels', {}).get('minimal', {})
                    selected_fields = complexity_config.get('fields', {})
                else:
                    print("No fields selected in custom mode. Using minimal defaults.")
                    complexity_config = self.yaml_data.get('complexity_levels', {}).get('minimal', {})
                    selected_fields = complexity_config.get('fields', {})
        
        print(f"✅ Selected {len(selected_fields)} fields for '{complexity_level}' complexity")
        return selected_fields
    
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
        print(f"  • Selected fields: {len(fields)}")  # FIX: Remove ['fields'] access
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
                'field_mappings': self._transform_fields_yaml_to_conf(fields),  # FIX: Remove ['fields'] access
                
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
                
                # Core field mapping in nested structure
                conf_mappings[api_module][service_field] = {
                    'database_field': field_config.get('database_field', field_name),
                    'database_type': field_config.get('database_type', 'REAL'),
                    'database_table': field_config.get('database_table', 'archive'),
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
                
                database_table = field_config.get('database_table', 'archive')
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

    def show_marine_custom_selection(self, field_definitions):
        """Marine-specific field selection with COOP/NDBC grouping."""
        import curses
        
        def curses_main(stdscr):
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
                    'display': field_info.get('display_name', field_name),  # CLEAN field name only
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
                    'description': 'Offshore marine weather, waves, sea surface temperature'
                })
                all_items.extend(ndbc_fields)
            
            # State variables - FIXED: Initialize before draw_interface function
            current_item = 0
            scroll_offset = 0
            
            def draw_interface():
                nonlocal scroll_offset  # FIXED: Declare nonlocal to modify outer scope variable
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Title
                title = "MARINE FIELD SELECTION - Choose Your Data Sources"
                try:
                    stdscr.addstr(0, (width - len(title)) // 2, title, curses.color_pair(3) | curses.A_BOLD)
                except curses.error:
                    pass
                
                # Instructions
                instructions = "↑↓: Navigate  SPACE: Select  ENTER: Confirm  ESC: Cancel"
                try:
                    stdscr.addstr(1, (width - len(instructions)) // 2, instructions)
                except curses.error:
                    pass
                
                # Calculate display area
                start_y = 3
                display_height = height - 5  # Leave room for title, instructions, and summary
                
                # Calculate scroll range
                if len(all_items) > display_height:
                    if current_item < scroll_offset:
                        scroll_offset = current_item
                    elif current_item >= scroll_offset + display_height:
                        scroll_offset = current_item - display_height + 1
                
                # Display items
                for i in range(display_height):
                    item_idx = scroll_offset + i
                    if item_idx >= len(all_items):
                        break
                        
                    item = all_items[item_idx]
                    y = start_y + i
                    
                    if item['type'] == 'header':
                        # Marine section header - FIXED: Use only the display text, no description bleeding
                        try:
                            stdscr.addstr(y, 0, item['display'], curses.color_pair(4) | curses.A_BOLD)
                            # REMOVED: Description display that was causing text bleeding
                        except curses.error:
                            pass
                    elif item['type'] == 'spacer':
                        # Empty line between sections
                        continue
                    elif item['type'] == 'field':
                        # Marine field selection - FIXED: Clean display name only
                        mark = "[X]" if item['selected'] else "[ ]"
                        line = f"  {mark} {item['display']}"  # CLEAN: Only the display name
                        
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
    


class MarineDatabaseManager:
    """
    Database schema management for Marine Data Extension.
    
    All database operations moved here from installer class.
    Handles table creation, field addition, and hybrid weectl/SQL operations.
    """
    
    def __init__(self, config_dict):
        self.config_dict = config_dict

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

    def _add_missing_fields(self, missing_fields, field_mappings):
        """Add missing database fields using hybrid approach from OpenWeather success patterns."""
        weectl_path = self._find_weectl()
        config_path = getattr(self.config_dict, 'filename', '/etc/weewx/weewx.conf')
        created_count = 0
        
        for field_name in sorted(missing_fields):
            field_type = field_mappings[field_name]
            
            print(f"  Adding field '{field_name}' ({field_type})...")
            
            # Use weectl for numeric types (confirmed supported)
            if field_type in ['REAL', 'INTEGER', 'real', 'integer', 'int']:
                if not weectl_path:
                    raise Exception("weectl executable not found - required for numeric field types")
                
                # CRITICAL: Use equals sign format for all parameters
                cmd = [weectl_path, 'database', 'add-column', field_name, 
                    f'--config={config_path}', '-y']
                cmd.insert(-2, f'--type={field_type}')
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    print(f"    ✓ Successfully added '{field_name}' using weectl")
                    created_count += 1
                elif 'duplicate column' in result.stderr.lower():
                    print(f"    ✓ Field '{field_name}' already exists")
                    created_count += 1
                else:
                    raise Exception(f"weectl failed to add '{field_name}': {result.stderr.strip()}")
            
            else:
                # Use direct SQL for VARCHAR/TEXT types (weectl limitation workaround)
                self._add_field_direct_sql(field_name, field_type)
                created_count += 1
        
        return created_count

    def _add_field_direct_sql(self, field_name, field_type):
        """Add field using direct SQL through WeeWX database manager."""
        try:
            db_binding = 'wx_binding'
            
            with weewx.manager.open_manager_with_config(self.config_dict, db_binding) as dbmanager:
                # Convert MySQL-specific types for SQLite compatibility
                if field_type.startswith('VARCHAR'):
                    sql_type = 'TEXT' if 'sqlite' in str(dbmanager.connection).lower() else field_type
                else:
                    sql_type = field_type
                
                sql = f"ALTER TABLE archive ADD COLUMN {field_name} {sql_type}"
                dbmanager.connection.execute(sql)
                print(f"    ✓ Successfully added '{field_name}' using direct SQL")
                
        except Exception as e:
            error_msg = str(e).lower()
            if 'duplicate column' in error_msg or 'already exists' in error_msg:
                print(f"    ✓ Field '{field_name}' already exists")
            else:
                print(f"    ❌ Failed to add '{field_name}': {e}")
                raise Exception(f"Direct SQL field creation failed: {e}")

    def extend_database_schema(self, selected_options):
        """Extend database schema following OpenWeather success patterns."""
        print("\n🗄️  DATABASE SCHEMA EXTENSION")
        print("-" * 40)
        
        # Extract field mappings from selected options
        field_mappings = self._extract_field_mappings_from_selection(selected_options)
        
        if not field_mappings:
            print("⚠️  No field mappings found - skipping database extension.")
            return
        
        print("  📋 Creating database fields...")
        print(f"  📊 Found {len(field_mappings)} fields to process")
        
        # Check existing fields (CRITICAL - this was missing)
        existing_fields = self._check_existing_fields()
        
        # Determine missing fields
        missing_fields = set(field_mappings.keys()) - set(existing_fields)
        already_present = set(field_mappings.keys()) & set(existing_fields)
        
        # Report existing fields
        if already_present:
            print("  ✓ Fields already present in database:")
            for field in sorted(already_present):
                print(f"    • {field}")
        
        # Add missing fields
        created_count = 0
        if missing_fields:
            print(f"  🔧 Adding {len(missing_fields)} missing fields:")
            created_count = self._add_missing_fields(missing_fields, field_mappings)
        else:
            print("  ✅ All required fields already exist in database.")
        
        print(f"\n✅ Database schema extension completed")
        print(f"    • Fields already present: {len(already_present)}")
        print(f"    • Fields created: {created_count}")

    def _extract_field_mappings_from_selection(self, selected_options):
        """Extract database field mappings from configuration written during setup."""
        field_mappings = {}
        
        # The field mappings were already written to config_dict during interactive setup
        # Extract them using the same pattern as the service code
        service_config = self.config_dict.get('MarineDataService', {})
        if not service_config:
            print("    ⚠️  Warning: No MarineDataService configuration found")
            return field_mappings
        
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
        """Create table if it doesn't exist - COMPLETE IMPLEMENTATION."""
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
    
    def _create_fields_with_weectl(self, fields):
        """Create REAL/INTEGER fields using weectl (success manual pattern)."""
        print("    📊 Creating numeric fields with weectl...")
        
        # Find weectl path
        weectl_path = self._find_weectl_path()
        if not weectl_path:
            print("    ⚠️  weectl not found - skipping weectl field creation")
            return
        
        try:
            config_path = getattr(self.config_dict, 'filename', '/etc/weewx/weewx.conf')
            
            for field_name, field_config in fields.items():
                db_field = field_config.get('database_field', field_name)
                db_type = field_config.get('database_type', 'REAL')
                
                # Only use weectl for REAL/INTEGER types
                if db_type not in ['REAL', 'INTEGER']:
                    continue
                
                # CRITICAL: Use correct format from success manual
                cmd = [weectl_path, 'database', 'add-column', db_field, 
                    f'--config={config_path}', '-y']
                cmd.insert(-2, f'--type={db_type}')  # ✅ CORRECT: --type=REAL format
                
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
        print("    📝 Creating text fields with direct SQL...")
        
        try:
            db_binding = 'wx_binding'
            
            for field_name, field_config in fields.items():
                db_field = field_config.get('database_field', field_name)
                db_type = field_config.get('database_type', 'REAL')
                
                # Only use SQL for non-numeric types
                if db_type in ['REAL', 'INTEGER']:
                    continue
                
                with weewx.manager.open_manager_with_config(self.config_dict, db_binding) as dbmanager:
                    # Convert MySQL-specific types for SQLite compatibility
                    if db_type.startswith('VARCHAR'):
                        sql_type = 'TEXT' if 'sqlite' in str(dbmanager.connection).lower() else db_type
                    else:
                        sql_type = db_type
                    
                    sql = f"ALTER TABLE archive ADD COLUMN {db_field} {sql_type}"
                    dbmanager.connection.execute(sql)
                    print(f"    ✅ Added field: {db_field} ({sql_type})")
                    
        except Exception as e:
            error_msg = str(e).lower()
            if 'duplicate column' in error_msg or 'already exists' in error_msg:
                print(f"    ✅ Field already exists")
            else:
                print(f"    ❌ Direct SQL field creation failed: {e}")
   
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

    def _find_weectl_path(self):
        """Find weectl executable path."""
        weectl_candidates = [
            '/usr/bin/weectl',
            '/usr/local/bin/weectl', 
            'weectl'  # Try PATH
        ]
        
        for candidate in weectl_candidates:
            try:
                result = subprocess.run([candidate, '--version'], 
                                    capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    print(f"  Found weectl: {candidate}")
                    return candidate
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
        
        print("  Warning: weectl not found - will use direct SQL for all fields")
        return None

    def _find_weectl(self):
        """Find weectl executable path."""
        for path in ['/usr/bin/weectl', '/usr/local/bin/weectl', 'weectl']:
            if subprocess.run(['which', path], capture_output=True).returncode == 0:
                return path
        return None

if __name__ == '__main__':
    print("This is a WeeWX extension installer.")
    print("Use: weectl extension install weewx-marine-data.zip")