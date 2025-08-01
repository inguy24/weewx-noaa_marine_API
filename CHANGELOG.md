# WeeWX Marine Data Extension - Changelog

All notable changes to this project will be documented in this file.

## [1.0.0-alpha] - 2025-07-31

### Added

#### Core Features
- **NOAA CO-OPS Integration**: Real-time water levels, tide predictions, and coastal water temperature
- **NDBC Buoy Integration**: Marine weather data including waves, wind, temperature, and pressure
- **Interactive Station Discovery**: Distance-based station selection within configurable radius
- **Modular Field Selection**: Choose between minimal, comprehensive, or custom field sets
- **Multi-Station Support**: Select multiple stations per data type for redundancy

#### Marine Data Fields
- **Tide Data**: Current water level, next high/low tide times and heights
- **Water Temperature**: Coastal water temperature from CO-OPS stations (where available)
- **Wave Data**: Significant wave height, dominant period, and wave direction
- **Marine Weather**: Wind speed/direction, air temperature, barometric pressure
- **Sea Conditions**: Sea surface temperature, visibility, atmospheric conditions

#### Installation & Configuration
- **Interactive Installer**: Station discovery and field selection during installation  
- **Automatic Database Setup**: Creates marine-specific database tables and fields
- **Configuration Validation**: Validates station availability and field compatibility
- **Service Registration**: Automatic WeeWX service registration and lifecycle management

### Technical Implementation
- **Base Service**: WeeWX 5.1 StdService inheritance with proper event binding
- **Two-Table Architecture**: Separate tables for high-frequency (CO-OPS) and forecast (NDBC) data
- **Background Threading**: Non-blocking data collection with configurable intervals
- **Unit System Integration**: Automatic unit system detection and field assignment
- **Error Handling**: Graceful degradation with retry logic and station fallback

### Dependencies
- **WeeWX**: 5.1 or later required
- **Python**: 3.7 or later
- **Network**: Internet connection for NOAA API access
- **Location**: US waters and territories (NOAA coverage area)

### Files Added
- `install.py` - Interactive extension installer
- `bin/user/marine_data.py` - Core service implementation
- `marine_data_fields.yaml` - Complete field definitions
- `README.md` - Installation and usage guide
- `CHANGELOG.md` - This version history
- `MANIFEST` - Package manifest file

### Alpha Release Notes

This is an **alpha version** for initial testing and feedback. 

**Testing Status:**
- Core functionality implemented and documented
- Service architecture follows proven WeeWX patterns
- Database operations designed for SQLite and MySQL
- Station discovery and field selection systems complete

**Feedback Requested:**
- Installation experience across different WeeWX setups
- Station selection and data collection performance
- Database field creation and data validation
- API usage patterns and rate limiting behavior

### Known Alpha Limitations
- **Limited Platform Testing**: Tested on limited WeeWX configurations
- **Error Edge Cases**: Some error scenarios may need refinement
- **Documentation**: May require updates based on user experience
- **Performance**: Real-world performance testing needed

## Future Development

Development will continue based on alpha testing feedback. Focus areas:

1. **Cross-Platform Testing**: Validate across different WeeWX installations
2. **Performance Optimization**: Fine-tune API intervals and error handling
3. **Documentation Updates**: Improve based on user feedback
4. **Additional Features**: Consider community-requested enhancements

## Support

- **Issues**: Report bugs and feature requests via GitHub Issues
- **Documentation**: Installation and configuration help in README.md
- **Community**: WeeWX user forums for general discussion

---

*Initial alpha release - July 31, 2025*