# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2024-02-03

### Added
- Multi-database support: PostgreSQL, MySQL, Oracle, SQLite (in addition to MS SQL Server)
- Optional dependencies for each database backend (`pip install schema-scraper[postgresql]`)
- Database name automatically included in output folder path
- `drivers` command to list available database drivers
- `test-connection` command to verify database connectivity
- Support for PostgreSQL-specific features:
  - Materialized views
  - Composite types, enum types, domain types, range types
  - PL/pgSQL functions and procedures
- Support for Oracle-specific features:
  - PL/SQL procedures and functions
  - Object types
  - Synonyms and sequences
- Support for MySQL-specific features:
  - Stored procedures and functions
  - Triggers
  - Check constraints (MySQL 8.0.16+)
- Support for SQLite:
  - Tables, views, triggers
  - Foreign key relationships
  - Index extraction

### Changed
- Renamed CLI command from `mssql-schema-scraper` to `schema-scraper`
- Output directory now includes database name: `./schema_docs/{database_name}/`
- Refactored to plugin-style backend architecture
- Updated minimum Python version to 3.10

## [0.1.0] - 2024-02-03

### Added
- Initial release with MS SQL Server support
- Extract tables, views, stored procedures, functions, triggers, user-defined types, sequences, synonyms
- Generate organized markdown documentation
- Support for Windows authentication and SQL Server authentication
- Schema filtering (include/exclude)
- Object type filtering
- Dry-run mode
- Verbose logging
