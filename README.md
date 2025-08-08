# WeeWX Marine Data Extension

**Real-time marine data for your WeeWX weather station from NOAA's official data sources**

## What This Extension Provides

### 🌊 Comprehensive Marine Data Collection

#### Tide Information (CO-OPS Data)
- **Real-time water levels** - Current observed water heights every 10 minutes
- **7-day rolling tide predictions** - Complete high/low tide forecasts with automatic cleanup
- **Coastal water temperature** - Temperature readings from equipped tide stations
- **Measurement quality indicators** - NOAA quality flags and accuracy estimates

#### Marine Weather (NDBC Buoy Data) 
- **Wave conditions** - Significant height, dominant period, and wave direction
- **Marine winds** - Speed, direction, and gust measurements at sea
- **Sea surface temperature** - Offshore water temperature readings
- **Marine atmosphere** - Barometric pressure, air temperature, visibility, dewpoint

#### Enhanced Features
- **Multi-station support** - Select multiple stations for redundancy and coverage
- **Automatic station discovery** - Distance-based station selection within configurable radius
- **WeeWX 5.1 compliant** - Uses native WeeWX database managers and service patterns
- **Background data collection** - Non-blocking collection with configurable intervals
- **Template integration** - TideTableSearchList for WeeWX template access

## Quick Start

### Prerequisites
- **WeeWX 5.1+** - Check with `weectl --version`
- **Python 3.7+** - Check with `python3 --version`
- **Internet connection** - Required for NOAA API access
- **US coastal or Great Lakes location** - Extension covers US coastal waters, Great Lakes, and territories

### Installation

1. **Download** the latest release:
   ```bash
   wget https://github.com/inguy24/weewx-noaa_marine_API/releases/download/v1.0.0-alpha/weewx-marine-data-1.0.0-alpha.zip
   ```

2. **Install** using WeeWX's extension system:
   ```bash
   sudo weectl extension install weewx-marine-data-1.0.0-alpha.zip
   ```

3. **Interactive Configuration** - The installer will guide you through:
   - **Station Discovery**: Automatic detection of nearby CO-OPS and NDBC stations
   - **Station Selection**: Choose multiple stations for backup coverage
   - **Field Selection**: Pick from essential or comprehensive data sets
   - **Database Setup**: Automatic creation of marine-specific tables

4. **Restart WeeWX**:
   ```bash
   sudo systemctl restart weewx
   ```

5. **Verify Installation**:
   ```bash
   # Test the installation
   python3 <USER_DIR>/marine_data.py --test-all
   
   # Check service status
   sudo systemctl status weewx
   ```

## Database Architecture

**1. coops_realtime** - High-frequency CO-OPS observations (10-minute updates)
```sql
-- Current water levels and coastal temperature
CREATE TABLE coops_realtime (
    dateTime INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    marine_current_water_level REAL,
    marine_water_level_sigma REAL,
    marine_water_level_flags TEXT,
    marine_coastal_water_temp REAL,
    marine_water_temp_flags TEXT,
    PRIMARY KEY (dateTime, station_id)
);
```

**2. tide_table** - 7-day rolling tide predictions (6-hour updates)
```sql
-- Complete tide forecast with automatic cleanup
CREATE TABLE tide_table (
    dateTime INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    tide_time INTEGER NOT NULL,
    tide_type TEXT NOT NULL,
    predicted_height REAL,
    datum TEXT,
    days_ahead INTEGER,
    PRIMARY KEY (station_id, tide_time, tide_type)
);
```

**3. ndbc_data** - NDBC buoy observations (hourly updates)
```sql
-- Marine weather and sea conditions
CREATE TABLE ndbc_data (
    dateTime INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    marine_wave_height REAL,
    marine_wave_period REAL,
    marine_wind_speed REAL,
    marine_wind_direction REAL,
    marine_sea_surface_temp REAL,
    marine_barometric_pressure REAL,
    -- Additional fields available
    PRIMARY KEY (dateTime, station_id)
);
```

## Station Selection Made Easy

### Intelligent Station Discovery
The installer automatically finds the best marine monitoring stations for your location:

**CO-OPS Tide Stations**:
- Distance-based discovery within 100 miles
- Station capability detection (water level, predictions, temperature)
- Multiple station selection for backup coverage
- Real-time availability checking

**NDBC Marine Weather Buoys**:
- Offshore and coastal buoy identification
- Comprehensive weather data availability
- Seasonal operation awareness (Great Lakes)
- Station metadata and depth information

### Station Selection Interface

**Interactive Curses-Based Selection**

The installer uses a modern curses interface with separate pages for each station type:

**Page 1: CO-OPS Tide Stations**
```
📍 CO-OPS Tide Stations
========================
Use arrow keys to navigate, SPACE to select/deselect, ENTER to continue
Select multiple stations for backup coverage during maintenance periods

[ ] Newport Harbor, RI (8452660) - 5.2 miles [RI]
    Capabilities: Water Level, Predictions, Water Temperature

[X] Point Judith, RI (8452951) - 8.1 miles [RI]  
    Capabilities: Water Level, Predictions

[ ] Block Island, RI (8452944) - 12.8 miles [RI]
    Capabilities: Water Level, Predictions

Selected: 1 | Station 2/8 | ENTER to continue
```

**Page 2: NDBC Marine Weather Buoys**
```
📍 NDBC Marine Weather Buoys
============================
Use arrow keys to navigate, SPACE to select/deselect, ENTER to continue

[X] Block Island Sound (44097) - 15.3 miles
    Capabilities: Waves, Wind, Temperature, Pressure

[ ] Montauk Point (44017) - 28.7 miles
    Capabilities: Waves, Wind, Temperature

Selected: 1 | Station 1/5 | ENTER to continue
```

## Field Selection Options

### Interactive Curses-Based Field Selection

**Single Unified Interface** with organized sections:

```
🔧 Marine Data Field Selection
==============================
Use arrow keys to navigate, SPACE to select/deselect, ENTER to continue
All fields selected by default - deselect unwanted fields

CO-OPS (Tides & Water Levels):
──────────────────────────────
  [X] Current water level
      → Current water level from NOAA sensors
  [X] Water level accuracy
      → Measurement accuracy estimate (±)
  [ ] Water level flags
      → Data quality control flags
  [X] Coastal water temperature
      → Water temperature at coastal stations (limited availability)
  [X] Next high tide time
      → Time of next high tide prediction
  [X] Next high tide level
      → Water level of next high tide
  [X] Next low tide time
      → Time of next low tide prediction
  [X] Next low tide level
      → Water level of next low tide

NDBC (Marine Weather):
──────────────────────
  [X] Significant wave height
      → Significant wave height (average of highest 1/3 of waves)
  [X] Dominant wave period
      → Dominant wave period
  [ ] Average wave period
      → Mean wave period
  [X] Marine wind speed
      → Wind speed
  [ ] Marine wind direction
      → Wind direction (degrees from true north)
  [X] Sea surface temperature
      → Sea surface water temperature
  [ ] Marine barometric pressure
      → Barometric pressure

Selected: 9/19 fields | Field 1/19 | ENTER to continue
```

### Field Selection Levels
- **All fields selected by default** - Deselect unwanted fields during installation
- **Organized by data source** - CO-OPS and NDBC sections clearly separated  
- **Real-time field descriptions** - Understand each field's purpose during selection
- **Flexible configuration** - Choose exactly the marine data you need

## WeeWX Template Integration

### TideTableSearchList Extension
**Automatic Registration**: The extension automatically registers the TideTableSearchList with WeeWX during installation, making tide data immediately available in templates.

Access tide data directly in WeeWX templates:

```html
<!-- Next tide information -->
<div class="tide-info">
    <h3>Next Tides</h3>
    #if $next_high_tide
    <p>Next High: $next_high_tide.formatted_time - $next_high_tide.formatted_height</p>
    #end if
    
    #if $next_low_tide  
    <p>Next Low: $next_low_tide.formatted_time - $next_low_tide.formatted_height</p>
    #end if
</div>

<!-- Today's tide schedule -->
<div class="today-tides">
    <h3>Today's Tides</h3>
    #for $tide in $today_tides
        <li>$tide.formatted_time - $tide.type: $tide.formatted_height</li>
    #end for
</div>
```

### Available Template Variables
- `$next_high_tide` / `$next_low_tide` - Next tide events
- `$today_tides` - Complete today's tide schedule  
- `$week_tides` - 7-day tide forecast organized by day
- `$tide_range_today` - Today's tidal range information

## Direct Database Access

### SQL Query Examples

**Get next 24 hours of tides:**
```sql
SELECT datetime(tide_time, 'unixepoch', 'localtime') as tide_time,
       tide_type, predicted_height, station_id
FROM tide_table 
WHERE tide_time > strftime('%s', 'now') 
AND tide_time < strftime('%s', 'now', '+24 hours')
ORDER BY tide_time;
```

**Current marine conditions:**
```sql
SELECT datetime(dateTime, 'unixepoch', 'localtime') as observation_time,
       station_id, marine_wave_height, marine_wind_speed, 
       marine_sea_surface_temp, marine_barometric_pressure
FROM ndbc_data 
WHERE dateTime > strftime('%s', 'now', '-2 hours')
ORDER BY dateTime DESC;
```

**Latest water levels:**
```sql
SELECT datetime(dateTime, 'unixepoch', 'localtime') as collection_time,
       station_id, marine_current_water_level, marine_coastal_water_temp
FROM coops_realtime 
WHERE dateTime > strftime('%s', 'now', '-2 hours')
ORDER BY dateTime DESC;
```

## Testing and Diagnostics

### MarineDataTester Class
**Built-in comprehensive testing** using the integrated MarineDataTester class:

```bash
# Test installation components
python3 <USER_DIR>/marine_data.py --test-install

# Test API connectivity  
python3 <USER_DIR>/marine_data.py --test-api

# Test database operations
python3 <USER_DIR>/marine_data.py --test-db

# Run comprehensive tests
python3 <USER_DIR>/marine_data.py --test-all
```

**Test Results Example:**
```
🧪 MARINE DATA EXTENSION - COMPREHENSIVE TESTING
================================================

🔧 TESTING INSTALLATION
-----------------------
Checking service registration...
  ✅ MarineDataService registered in data_services
Checking service configuration...
  ✅ MarineDataService configuration found
Checking database tables...
  ✅ coops_realtime table exists
  ✅ tide_table table exists  
  ✅ ndbc_data table exists

Installation Test: ✅ PASSED

🌐 TESTING API CONNECTIVITY
---------------------------
Testing CO-OPS API...
  ✅ CO-OPS API responding correctly
Testing NDBC API...
  ✅ NDBC API responding correctly

API Connectivity Test: ✅ PASSED

🔧 TESTING DATABASE OPERATIONS
------------------------------
Testing table structure...
  ✅ coops_realtime: 150 records
  ✅ tide_table: 84 records
  ✅ ndbc_data: 72 records
Testing database write/read operations...
  ✅ Database write/read operations working

Database Operations Test: ✅ PASSED

🔧 TEST SUMMARY
---------------
Overall: 3/3 tests passed
✅ ALL TESTS PASSED!
```

### Monitoring Data Collection
```bash
# Monitor marine data activity
sudo journalctl -u weewx -f | grep -i marine

# Check for successful data collection
sudo journalctl -u weewx -f | grep "Marine.*collected"

# Monitor for errors
sudo journalctl -u weewx -f | grep -i "error\|warning" | grep -i marine
```

## Configuration Management

### Station Reconfiguration
Change your station selection anytime:
```bash
# Reconfigure stations and fields
sudo weectl extension reconfigure marine_data

# View current configuration
sudo weectl extension show marine_data
```

### Update Intervals
Customize data collection frequency in `weewx.conf`:
```ini
[MarineDataService]
    [[collection_intervals]]
        coops_collection_interval = 600      # 10 minutes for water levels
        tide_predictions_interval = 21600    # 6 hours for tide predictions  
        ndbc_weather_interval = 3600         # 1 hour for marine weather
```

### API Usage Management
Monitor and optimize API usage:
- **Automatic monitoring**: Daily usage tracking with NOAA rate limits
- **Smart intervals**: Configurable collection frequencies
- **Multiple stations**: Balanced load across selected stations
- **Usage reporting**: Check logs for daily API call summaries

## Troubleshooting

### Common Installation Issues

**Service Registration Check:**
```bash
# Verify service is registered
grep "user.marine_data.MarineDataService" /etc/weewx/weewx.conf
```

**Database Table Verification:**
```bash
# Check marine tables exist
weectl database query "SHOW TABLES" | grep -E "(coops_realtime|tide_table|ndbc_data)"
```

**Data Collection Verification:**
```bash
# Check recent data collection
weectl database query "SELECT COUNT(*) FROM coops_realtime WHERE dateTime > strftime('%s', 'now', '-1 day')"
```

### Marine Station Status
When experiencing data gaps, check official NOAA status:
- **CO-OPS Stations**: [NOAA Tides & Currents](https://tidesandcurrents.noaa.gov/stations.html)
- **NDBC Buoys**: [National Data Buoy Center](https://www.ndbc.noaa.gov/)

### Configuration Validation
```bash
# Test configuration validity
python3 /usr/share/weewx/user/marine_data.py --test-install

# Check service configuration
python3 -c "
import configobj
config = configobj.ConfigObj('/etc/weewx/weewx.conf')
print('MarineDataService configured:', 'MarineDataService' in config)
"
```

## Performance and Optimization

### Database Performance
- **Optimized indexes**: Automatic creation for common query patterns
- **Data cleanup**: Automatic removal of expired tide predictions
- **Efficient queries**: Sub-second response times for template integration
- **Minimal storage**: ~10MB growth per month per station

### API Efficiency
- **Smart scheduling**: Optimal collection intervals for each data type
- **Error recovery**: Automatic retry with exponential backoff
- **Station health**: Background monitoring and restart capability
- **Rate limiting**: Built-in NOAA API usage compliance

## Advanced Features

### Background Thread Health Monitoring
- **Automatic restart**: Dead thread detection and recovery
- **Progress tracking**: Last successful collection timestamps
- **Error escalation**: Configurable retry and recovery procedures
- **Service continuity**: WeeWX service protection during marine data issues

### Multi-Station Redundancy
- **Backup coverage**: Multiple stations prevent service interruption
- **Maintenance awareness**: Handles NOAA station maintenance periods
- **Geographic coverage**: Select stations for optimal area coverage
- **Data validation**: Cross-station data consistency checking

### Unit System Integration
- **Automatic detection**: Uses WeeWX configured unit system
- **Consistent conversions**: NOAA metric to WeeWX unit standards
- **Template compatibility**: Native WeeWX unit group assignments
- **Database optimization**: Efficient storage with proper typing



## 🗑️ Uninstallation

```bash
# Remove the extension completely
sudo weectl extension uninstall marine_data

# Restart WeeWX
sudo systemctl restart weewx
```

**Note**: Uninstallation preserves collected marine data in the database tables.

## 🤝 Contributing

### Development Setup
```bash
# Clone the repository
git clone https://github.com/inguy24/weewx-noaa_marine_API.git
cd weewx-noaa_marine_API

# Install in development mode
sudo weectl extension install .

# Run tests
python3 /usr/share/weewx/user/marine_data.py --test-all
```

### Code Standards
- Follow WeeWX 5.1 service and extension patterns
- Use WeeWX database managers (no custom connections)
- Implement graceful degradation for API failures
- Comprehensive error handling with logging
- Performance optimization for continuous operation

## 📄 License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

Copyright (C) 2025 Shane Burkhardt - [GitHub Profile](https://github.com/inguy24)

## 🙏 Acknowledgments

- **WeeWX Community** - For the excellent weather station software and development guidance
- **NOAA** - For providing free access to comprehensive marine data APIs  
- **WeeWX 5.1 Documentation** - Architectural patterns and database management guidance
- **Marine Data Users** - Testing feedback and real-world validation

## 📞 Support

- **Bug Reports**: [GitHub Issues](https://github.com/inguy24/weewx-noaa_marine_API/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/inguy24/weewx-noaa_marine_API/issues) with enhancement label
- **Documentation**: [Project Wiki](https://github.com/inguy24/weewx-noaa_marine_API/wiki)
- **WeeWX Help**: [WeeWX User Group](https://groups.google.com/g/weewx-user)

## 🗺️ Coverage

- **US Coastal Waters** - All US states with coastlines
- **US Territories** - Puerto Rico, US Virgin Islands, Guam, American Samoa  
- **Great Lakes** - Seasonal coverage (stations may be removed during ice season)

**Note**: Coverage limited to NOAA data sources (US waters and territories).

---

**Version**: 1.0.0-alpha | **WeeWX Compatibility**: 5.1+ | **License**: GPL v3.0 | **Magic Animal**: 🐷