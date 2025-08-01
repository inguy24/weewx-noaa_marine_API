# WeeWX Marine Data Extension

Add real-time marine data to your WeeWX weather station from NOAA's official data sources.

## What This Extension Provides

### 🌊 Tide Information
Get accurate tide data for any US coastal location:
- **Current water level** - Real-time observed water heights
- **Tide predictions** - Next high and low tide times and heights  
- **Coastal water temperature** - Water temperature from tide stations (where available)

### 🌊 Marine Weather
Access offshore weather conditions from NOAA buoys:
- **Wave conditions** - Wave height, period, and direction
- **Marine winds** - Wind speed, direction, and gusts at sea
- **Sea temperature** - Sea surface temperature readings
- **Marine atmosphere** - Barometric pressure, air temperature, visibility

## Quick Start

### Prerequisites
- **WeeWX 5.1+** - Check with `weectl --version`
- **Python 3.7+** - Check with `python3 --version`
- **Internet connection** - Required for NOAA API access
- **US coastal or Great Lakes location** - Extension covers US coastal waters, Great Lakes, and territories

### Installation

1. **Download** the latest release from [GitHub Releases](https://github.com/inguy24/weewx-noaa_marine_API/releases)
2. **Install** using WeeWX's extension system:
   ```bash
   sudo weectl extension install weewx-marine-data-1.0.0-alpha.zip
   ```
3. **Follow the interactive installer** - it will guide you through:
   - Station discovery within your specified radius
   - Field selection (minimal, standard, or custom)
   - Configuration validation and database setup
4. **Restart WeeWX**:
   ```bash
   sudo systemctl restart weewx
   ```
5. **Verify installation** - your marine data will begin appearing in logs and reports

### Station Selection Made Easy

The installer automatically finds marine monitoring stations near you:

**For Tide Data:**
- Shows the 5 closest NOAA tide stations within 100 miles
- Displays station names, distances, and available data types
- Recommends stations with both water level observations and tide predictions

**For Marine Weather:**
- Finds nearby NOAA weather buoys within your specified range
- Shows buoy locations, depths, and types of measurements available
- Helps you choose between coastal and offshore monitoring points

### Choose Your Data

Pick from three levels of marine data collection:

**🎯 Minimal** - Essential marine monitoring
- Current water level and next tide times
- Basic wave height and sea surface temperature
- *~150 API calls per day*

**📊 Standard** - Comprehensive marine conditions  
- All tide predictions and water level data
- Complete wave analysis and marine meteorology
- *~400 API calls per day*

**⚙️ Custom** - Select exactly what you want
- Choose specific fields from each data source
- Optimize for your particular interests or location
- *Usage varies based on selection*

## Detailed Installation Guide

### Step 1: Download and Install

```bash
# Download from GitHub releases
wget https://github.com/inguy24/weewx-noaa_marine_API/releases/download/v1.0.0-alpha/weewx-marine-data-1.0.0-alpha.zip

# Install the extension
sudo weectl extension install weewx-marine-data-1.0.0-alpha.zip
```

### Step 2: Interactive Configuration

The installer will guide you through selecting your marine monitoring setup:

**Station Discovery Process:**
```
MARINE DATA EXTENSION INSTALLATION
==================================
Searching for marine stations near your location...

Found 8 CO-OPS tide stations within 100 miles:
1. Newport Harbor, RI (Station: 8452660) - 5.2 miles
   Products: Water Level, Predictions, Water Temperature
2. Point Judith, RI (Station: 8452951) - 8.1 miles  
   Products: Water Level, Predictions
3. Block Island, RI (Station: 8452944) - 12.8 miles
   Products: Water Level, Predictions

Select stations for tide data (1,2,3 or 'all'): 1,2

Found 5 NDBC buoys within 100 miles:
1. Block Island Sound (Buoy: 44097) - 15.3 miles
   Data: Waves, Wind, Temperature, Pressure  
2. Montauk Point (Buoy: 44017) - 28.7 miles
   Data: Waves, Wind, Temperature

Select stations for marine weather (1,2 or 'all'): 1,2

RECOMMENDATION: You have selected multiple stations for each data type.
This provides backup coverage when stations go offline for maintenance
or during severe weather conditions. You can reconfigure station 
selection anytime using: sudo weectl extension reconfigure marine_data
```

**Field Selection Process:**
```
FIELD SELECTION - Marine Data Extension
=======================================
Choose your data collection level:

1. MINIMAL - Essential marine monitoring (150 API calls/day)
   ✓ Current water level and tide predictions
   ✓ Basic wave height and sea surface temperature

2. STANDARD - Comprehensive marine conditions (400 API calls/day)  
   ✓ Complete tide analysis and water level monitoring
   ✓ Full marine meteorology and wave analysis

3. CUSTOM - Select specific fields
   ✓ Choose exactly what data you want

Selection (1/2/3): 3
```

**Custom Field Selection Interface:**
```
CUSTOM FIELD SELECTION - Marine Data Extension
===============================================

Use arrow keys to navigate, SPACE to select/deselect, ENTER to continue

NOAA TIDES & CURRENTS MODULE
────────────────────────────
Real-time Water Data (10-minute updates):
[x] Current water level
[ ] Water level measurement accuracy  
[x] Coastal water temperature (limited stations)

Tide Predictions (6-hour updates):
[x] Next high tide time
[x] Next high tide height
[x] Next low tide time  
[x] Next low tide height
[ ] Tidal range

NDBC MARINE WEATHER MODULE
──────────────────────────
Wave Conditions (hourly updates):
[x] Significant wave height
[x] Dominant wave period
[ ] Average wave period
[ ] Mean wave direction

Marine Meteorology (hourly updates):
[x] Sea surface temperature
[x] Marine wind speed
[ ] Marine wind direction
[ ] Wind gusts
[ ] Air temperature
[ ] Barometric pressure
[ ] Visibility

Selected: 9/19 total fields
Estimated API calls per day: ~250

[TAB] Switch sections  [SPACE] Select  [ENTER] Continue  [Q] Quit
```

### Step 3: Verify Installation

```bash
# Check service registration
sudo journalctl -u weewx -f | grep -i marine

# Test marine data collection
sudo python3 /usr/share/weewx/user/marine_data.py --test-api

# Monitor for successful data collection
sudo journalctl -u weewx -f | grep "Marine.*collected"
```

## Marine Data Fields Reference

Once installed, these fields will be available in your WeeWX database and reports:

### Tide & Water Level Data
| Field | Description | Updates |
|-------|-------------|---------|
| `marine_current_water_level` | Current observed water level | Every 10 minutes |
| `marine_next_high_time` | Next predicted high tide time | Every 6 hours |
| `marine_next_high_height` | Next predicted high tide height | Every 6 hours |
| `marine_next_low_time` | Next predicted low tide time | Every 6 hours |
| `marine_next_low_height` | Next predicted low tide height | Every 6 hours |
| `marine_coastal_water_temp` | Coastal water temperature | Every 10 minutes |

### Marine Weather Data  
| Field | Description | Updates |
|-------|-------------|---------|
| `marine_wave_height` | Significant wave height | Every hour |
| `marine_wave_period` | Dominant wave period | Every hour |
| `marine_wave_direction` | Mean wave direction | Every hour |
| `marine_wind_speed` | Marine wind speed | Every hour |
| `marine_wind_direction` | Marine wind direction | Every hour |
| `marine_sea_surface_temp` | Sea surface temperature | Every hour |
| `marine_air_temp` | Marine air temperature | Every hour |
| `marine_barometric_pressure` | Marine barometric pressure | Every hour |

## Station Selection Guide

### Choosing Tide Stations

**Distance Matters**: Tide timing can vary significantly along a coastline
- **Within 20 miles**: Excellent accuracy for tide predictions
- **20-50 miles**: Good accuracy, minor timing differences
- **50+ miles**: Useful for general trends, timing may vary

**Station Types**: Look for stations with these capabilities
- **Water Level Observations**: Real-time current conditions
- **Tide Predictions**: Calculated high/low tide times
- **Water Temperature**: Available at selected coastal stations

**Multiple Station Strategy**: Select 2-3 stations for reliability
- **Primary**: Closest station with all desired data
- **Backup**: Alternative stations in case of outages or maintenance
- **Redundancy**: Marine stations occasionally go offline for equipment maintenance, severe weather, or technical issues

### Choosing Marine Weather Buoys

**Buoy Location Types**:
- **Coastal buoys** (< 50 miles offshore) - Local nearshore conditions
- **Offshore buoys** (> 50 miles offshore) - Open ocean conditions  
- **Deep water buoys** (> 200m depth) - True offshore marine weather
- **Great Lakes buoys** - Seasonal operation, typically April-November

**Data Considerations**:
- **Wave data**: Most accurate from deep water locations
- **Wind data**: Coastal buoys may be influenced by land effects
- **Temperature**: Sea surface temperature varies with depth and currents

**Backup Buoy Strategy**: Select multiple buoys for continuous coverage
- **Primary**: Closest buoy with required measurements  
- **Secondary**: Alternative buoy for data validation and backup
- **Seasonal**: Great Lakes buoys may be removed during ice season

**Station Maintenance Periods**: 
Marine monitoring equipment requires regular maintenance. NOAA typically schedules:
- **Routine maintenance**: 2-4 times per year per station
- **Equipment upgrades**: May cause extended outages
- **Weather-related**: Severe storms can damage or disable equipment

**Reconfiguration Options**:
```bash
# Reconfigure station and field selection anytime
sudo weectl extension reconfigure marine_data

# View current station status and selection
sudo weectl extension show marine_data
```

## Configuration Examples

### Coastal Monitoring Setup
Perfect for harbors, beaches, and coastal activities:
```
Selected Stations:
- Tide Station: Newport Harbor, RI (5.2 miles)
- Marine Buoy: Block Island, RI (12.8 miles)

Selected Fields:
- Current water level and tide predictions
- Wave height and sea surface temperature  
- Marine wind speed and direction
```

### Offshore Fishing Setup
Optimized for offshore conditions and fishing:
```  
Selected Stations:
- Tide Station: Point Judith, RI (8.1 miles) 
- Marine Buoy: Nantucket Sound (45.2 miles)

Selected Fields:
- Basic tide information
- Complete wave analysis (height, period, direction)
- Sea surface temperature and marine winds
- Barometric pressure trends
```

## Troubleshooting

### No Marine Data Appearing

**Check station connectivity:**
```bash
# Test your selected stations
sudo -u weewx python3 /usr/share/weewx/user/marine_data.py --test-api
```

**Verify configuration:**
- Station IDs are correct and active
- Selected fields are supported by your stations
- Network connectivity to NOAA APIs

### Intermittent Data Gaps

Marine monitoring stations occasionally go offline for maintenance or due to weather conditions. This is normal.

**What the extension does automatically:**
- Logs station outages and data gaps
- Continues collecting from available backup stations
- Resumes data collection when stations come back online

**When to take action:**
- Gaps longer than 6-12 hours may indicate station issues
- Check [NOAA CO-OPS Station Status](https://tidesandcurrents.noaa.gov/stations.html) 
- Check [NDBC Buoy Status](https://www.ndbc.noaa.gov/)

### High API Usage Warnings

The extension monitors your daily API usage to stay within NOAA's reasonable use policies.

**Reduce usage by:**
- Selecting fewer fields or stations
- Increasing update intervals in configuration
- Choosing "minimal" field selection during reconfiguration

**Current usage:** Check your WeeWX logs for daily API usage summaries.

## 🗑️ Uninstallation

```bash
# Remove the extension completely
sudo weectl extension uninstall marine_data

# Restart WeeWX
sudo systemctl restart weewx
```

**Note**: Uninstallation removes the service and configuration but preserves your collected marine data in the database.

## 🛠️ Troubleshooting

### Common Issues

**Installation Problems:**
- Ensure WeeWX 5.1+ is installed: `weectl --version`
- Check Python version: `python3 --version` (3.7+ required)
- Verify internet connectivity for station discovery
- Check permissions: installer must run as root/sudo

**Station Discovery Issues:**
- **No stations found**: Increase search radius or check coastal proximity
- **Station unavailable**: Some stations may be temporarily offline - select backups
- **Invalid coordinates**: Verify WeeWX station location in configuration

**Data Collection Issues:**
- **No marine data appearing**: Check service status and API connectivity
- **Intermittent data gaps**: Normal for marine stations during maintenance
- **High API usage**: Reduce selected fields or increase collection intervals

**Database Issues:**
- **Field creation errors**: Check WeeWX database write permissions
- **Missing data columns**: Re-run installer or create fields manually
- **Database conflicts**: Avoid installing over existing marine field modifications

### Log Monitoring

```bash
# Monitor WeeWX logs for marine data activity
sudo journalctl -u weewx -f | grep -i marine

# Check for successful data collection
sudo journalctl -u weewx -f | grep "Marine.*collected"

# Monitor for errors and warnings
sudo journalctl -u weewx -f | grep -i "error\|warning" | grep -i marine
```

### Debug Configuration

Enable detailed logging in `weewx.conf`:

```ini
[MarineDataService]
    log_success = true
    log_errors = true

[Logging]
    [[loggers]]
        [[[user.marine_data]]]
            level = DEBUG
```

### Diagnostic Commands

```bash
# Test installation and service registration
PYTHONPATH=/usr/share/weewx:/etc/weewx/bin/user python3 /usr/share/weewx/user/marine_data.py --test-install

# Test station connectivity and API access
PYTHONPATH=/usr/share/weewx:/etc/weewx/bin/user python3 /usr/share/weewx/user/marine_data.py --test-api

# Run comprehensive system tests
PYTHONPATH=/usr/share/weewx:/etc/weewx/bin/user python3 /usr/share/weewx/user/marine_data.py --test-all
```

### Manual Database Field Creation

If automatic field creation fails, create fields manually:

```bash
# Create marine data fields manually
weectl database add-column marine_current_water_level --type REAL -y
weectl database add-column marine_next_high_time --type INTEGER -y
weectl database add-column marine_wave_height --type REAL -y
weectl database add-column marine_sea_surface_temp --type REAL -y

# Restart WeeWX after manual field creation
sudo systemctl restart weewx
```

### Station Status Checking

When experiencing data gaps, check official NOAA station status:

- **CO-OPS Stations**: [NOAA Tides & Currents](https://tidesandcurrents.noaa.gov/stations.html)
- **NDBC Buoys**: [National Data Buoy Center](https://www.ndbc.noaa.gov/)

Look for status indicators and recent data timestamps to verify station operation.

## API Usage Management

### Understanding API Usage

The extension automatically monitors daily API usage to stay within NOAA's reasonable use policies:

**API Call Estimates:**
- **Minimal Configuration**: ~150 calls/day (conservative)
- **Standard Configuration**: ~400 calls/day (typical) 
- **Custom Configuration**: Varies based on field selection

**Current Usage Monitoring:**
Check your WeeWX logs for daily API usage summaries:
```bash
sudo journalctl -u weewx | grep "Marine.*API usage"
```

### Optimizing API Usage

**Reduce usage by:**
- Selecting fewer stations (choose 1-2 primary stations)
- Using "minimal" field selection level
- Increasing collection intervals:
  ```ini
  [[collection_intervals]]
      coops_water_level = 1800      # 30 minutes instead of 10
      ndbc_weather = 7200          # 2 hours instead of 1
  ```

**Balance responsiveness vs usage:**
- **Tide monitoring**: 10-30 minute intervals work well
- **Marine weather**: 1-2 hour intervals sufficient for most needs
- **Tide predictions**: 6-12 hour intervals (predictions change slowly)

### Custom Update Intervals

After installation, you can adjust data collection timing in `weewx.conf`:

```ini
[MarineDataService]
    enable = true
    
    [[collection_intervals]]
        coops_water_level = 600        # 10 minutes (600 seconds)
        coops_predictions = 21600      # 6 hours
        ndbc_weather = 3600           # 1 hour
```

### Multiple Station Management

The extension handles multiple stations automatically, but you can prioritize them:

```ini
[MarineDataService]
    [[selected_stations]]
        [[[coops_stations]]]
            9410230 = true    # Primary: La Jolla, CA
            9410580 = true    # Backup: Newport Beach, CA
        [[[ndbc_stations]]]
            46087 = true      # Primary: Offshore buoy
            46025 = true      # Backup: Coastal buoy
```

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/inguy24/weewx-noaa_marine_API.git
cd weewx-noaa_marine_API

# Install in development mode
sudo weectl extension install .

# Run comprehensive tests
python3 /usr/share/weewx/user/marine_data.py --test-all

# Test specific functionality
PYTHONPATH=/usr/share/weewx:/etc/weewx/bin/user python3 /usr/share/weewx/user/marine_data.py --test-api
```

### Reporting Issues

When reporting issues, please include:
- WeeWX version (`weectl --version`)
- Python version (`python3 --version`)
- Operating system and version
- Extension configuration (remove sensitive data)
- Relevant log entries with timestamps
- Steps to reproduce the issue

### Code Standards

- Follow PEP 8 Python style guidelines
- Add comprehensive docstrings to functions and classes
- Include error handling with graceful degradation
- Test changes with real WeeWX installations
- Update documentation for new features

## 📄 License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

Copyright (C) 2025 Shane Burkhardt - [GitHub Profile](https://github.com/inguy24)

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

## 🙏 Acknowledgments

- **WeeWX Community** - For the excellent weather station software and support
- **NOAA** - For providing free access to comprehensive marine data APIs
- **Alpha Testers** - Community members helping test and improve this extension
- **OpenWeather Extension** - Architectural patterns and best practices reference

## 📞 Support

- **Bug Reports**: [GitHub Issues](https://github.com/inguy24/weewx-noaa_marine_API/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/inguy24/weewx-noaa_marine_API/issues) with enhancement label
- **Discussions**: [GitHub Discussions](https://github.com/inguy24/weewx-noaa_marine_API/discussions)
- **Documentation**: [Project Wiki](https://github.com/inguy24/weewx-noaa_marine_API/wiki)
- **WeeWX Help**: [WeeWX User Group](https://groups.google.com/g/weewx-user)

## 🔄 Version History

See [CHANGELOG.md](CHANGELOG.md) for detailed version history and release notes.

## 🗺️ Coverage

This extension provides data for:
- **US Coastal Waters** - All US states with coastlines
- **US Territories** - Puerto Rico, US Virgin Islands, Guam, American Samoa
- **Great Lakes** - Coverage varies by season and station availability

**Note**: Limited coverage outside US waters due to NOAA data source focus.

---

**Version**: 1.0.0-alpha | **WeeWX Compatibility**: 5.1+ | **License**: GPL v3.0 | **Author**: [Shane Burkhardt](https://github.com/inguy24)