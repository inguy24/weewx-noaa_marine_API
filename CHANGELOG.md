# WeeWX Marine Data Extension - Changelog

All notable changes to this project will be documented in this file.

## [1.0.1-beta] - 2025-08-14

### Fixed
- Database table creation methods now use YAML field definitions instead of hardcoded values
- Corrected field mapping generation to match actual database schema
- Fixed `_create_ndbc_data_table()`, `_create_tide_table()`, and `_create_coops_realtime_table()` to be data-driven
- Resolved field name mismatches between CONF mappings and database tables

### Changed
- Table creation methods now read field definitions from marine_data_fields.yaml
- Improved WeeWX 5.1 database manager compliance

## [1.0.0-alpha] - 2025-08-08

### Added
- Initial release of NOAA Marine Data Extension
- Interactive station selection and field configuration
- Three-table database architecture (coops_realtime, tide_table, ndbc_data)
- Background data collection threads
- WeeWX 5.1 integration

---

**Current Version**: 1.0.1-beta  
**WeeWX Compatibility**: 5.1+  
**License**: GPL v3.0