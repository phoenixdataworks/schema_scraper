# Schema Scraper

[![PyPI version](https://badge.fury.io/py/schema-scraper.svg)](https://badge.fury.io/py/schema-scraper)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python CLI tool that connects to databases, extracts comprehensive schema metadata, and generates organized markdown documentation suitable for AI consumption and version control.

## Supported Databases

| Database | Driver | Installation |
|----------|--------|--------------|
| MS SQL Server | pyodbc | `pip install schema-scraper[mssql]` |
| PostgreSQL | psycopg3 | `pip install schema-scraper[postgresql]` |
| MySQL/MariaDB | mysql-connector-python | `pip install schema-scraper[mysql]` |
| Oracle | oracledb | `pip install schema-scraper[oracle]` |
| SQLite | built-in sqlite3 | `pip install schema-scraper` |

## Installation

```bash
# Base installation (SQLite only)
pip install schema-scraper

# With specific database support
pip install schema-scraper[mssql]
pip install schema-scraper[postgresql]
pip install schema-scraper[mysql]
pip install schema-scraper[oracle]

# With all database support
pip install schema-scraper[all]

# Development installation
pip install -e ".[all,dev]"
```

### Driver Requirements

**MS SQL Server:**
```bash
# macOS
brew install microsoft/mssql-release/msodbcsql18

# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Windows
# Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

**Oracle:**
```bash
# Oracle Instant Client may be required for some features
# See: https://www.oracle.com/database/technologies/instant-client.html
```

## Quick Start

```bash
# SQLite (no extra dependencies)
schema-scraper scrape -t sqlite -d ./mydb.sqlite

# PostgreSQL
schema-scraper scrape -t postgresql -h localhost -d mydb -u postgres -p password

# MS SQL Server with Windows auth
schema-scraper scrape -t mssql -h localhost -d MyDatabase --trusted

# MySQL
schema-scraper scrape -t mysql -h localhost -d mydb -u root -p password
```

Output will be generated in `./schema_docs/{database_name}/`.

## Usage

### CLI Options

```
Usage: schema-scraper scrape [OPTIONS]

Options:
  -t, --db-type [mssql|postgresql|mysql|oracle|sqlite]
                                  Database type (default: mssql)
  -h, --host TEXT                 Database server hostname
  -P, --port INTEGER              Database server port
  -d, --database TEXT             Database name (or file path for SQLite)
  -u, --username TEXT             Database username
  -p, --password TEXT             Database password
  --trusted                       Use Windows authentication (MSSQL only)
  -c, --connection-string TEXT    Full connection string (MSSQL only)
  --driver TEXT                   ODBC driver name (MSSQL only)
  --service-name TEXT             Oracle service name
  --sid TEXT                      Oracle SID
  -o, --output PATH               Output base directory (default: ./schema_docs)
  --schemas TEXT                  Include only specific schemas (repeatable)
  --exclude-schemas TEXT          Exclude specific schemas (repeatable)
  --object-types [tables|views|procedures|functions|triggers|types|sequences|synonyms|all]
                                  Object types to extract (default: all)
  -v, --verbose                   Increase verbosity (-v info, -vv debug)
  --dry-run                       Preview without writing files
  --help                          Show this message and exit.
```

### Environment Variables

Connection parameters can be set via environment variables:

| Variable | Description |
|----------|-------------|
| `DB_HOST` | Database host |
| `DB_PORT` | Database port |
| `DB_NAME` | Database name |
| `DB_USER` | Username |
| `DB_PASSWORD` | Password |
| `DB_CONNECTION_STRING` | Full connection string (MSSQL) |

### Additional Commands

```bash
# List available database drivers
schema-scraper drivers

# Test database connection
schema-scraper test-connection -t postgresql -h localhost -d mydb -u user -p pass
```

## Output Structure

```
schema_docs/{database_name}/
├── README.md                        # Database overview + navigation
├── tables/
│   ├── README.md                    # Table index
│   └── {schema}.{table}.md          # Per-table documentation
├── views/
│   ├── README.md
│   └── {schema}.{view}.md
├── procedures/
│   ├── README.md
│   └── {schema}.{procedure}.md
├── functions/
│   ├── README.md
│   └── {schema}.{function}.md
├── triggers/
│   ├── README.md
│   └── {schema}.{trigger}.md
├── types/
│   ├── README.md
│   └── {schema}.{type}.md
├── sequences/
│   ├── README.md
│   └── {schema}.{sequence}.md
├── synonyms/
│   ├── README.md
│   └── {schema}.{synonym}.md
└── schemas/
    ├── README.md
    └── {schema}.md                  # All objects per schema
```

## What Gets Documented

### Tables
- Full column list with data types, nullability, defaults, identity, computed columns
- Primary key (name, columns, clustered/non-clustered)
- Foreign keys (columns, referenced table, ON DELETE/UPDATE actions)
- All indexes (type, columns, included columns, filters)
- Check constraints with full definitions
- Row count and space usage statistics
- Relationship graph (what references this table, what it references)

### Views
- Column list with data types
- Full SQL definition
- Base tables referenced
- Materialized view indicator (PostgreSQL)

### Stored Procedures
- All parameters with types, direction (IN/OUT), defaults
- Full SQL definition
- Language (T-SQL, PL/SQL, PL/pgSQL, etc.)

### Functions
- Function type (scalar, table-valued, aggregate, window)
- Parameters with types and defaults
- Return type (scalar) or return columns (table-valued)
- Full SQL definition

### Triggers
- Trigger type (BEFORE/AFTER/INSTEAD OF)
- Events (INSERT, UPDATE, DELETE)
- Parent table
- Full SQL definition
- Disabled status

### User-Defined Types
- Type category (domain, composite, enum, table type, alias)
- Base type and constraints
- Column definitions (for composite/table types)
- Enum values (PostgreSQL)

### Sequences
- Data type and range (min/max values)
- Increment, start value, current value
- Cycling behavior
- Cache settings

### Synonyms
- Base object reference
- Target server/database/schema/object

## Database Feature Support

| Feature | MSSQL | PostgreSQL | MySQL | Oracle | SQLite |
|---------|:-----:|:----------:|:-----:|:------:|:------:|
| Tables | ✅ | ✅ | ✅ | ✅ | ✅ |
| Views | ✅ | ✅ | ✅ | ✅ | ✅ |
| Materialized Views | ❌ | ✅ | ❌ | ❌ | ❌ |
| Stored Procedures | ✅ | ✅ | ✅ | ✅ | ❌ |
| Functions | ✅ | ✅ | ✅ | ✅ | ❌ |
| Triggers | ✅ | ✅ | ✅ | ✅ | ✅ |
| User-Defined Types | ✅ | ✅ | ❌ | ✅ | ❌ |
| Sequences | ✅ | ✅ | ❌ | ✅ | ❌ |
| Synonyms | ✅ | ❌ | ❌ | ✅ | ❌ |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Install development dependencies (`pip install -e ".[all,dev]"`)
4. Make your changes
5. Run tests (`pytest`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a list of changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
