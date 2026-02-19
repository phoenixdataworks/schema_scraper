"""Click CLI interface for the schema scraper."""

import logging
import sys
from pathlib import Path

import click

from . import SUPPORTED_BACKENDS, __version__
from .backends import get_backend
from .base.models import Database
from .config import ScraperConfig
from .exceptions import (
    BackendNotAvailableError,
    ConfigurationError,
    ConnectionError,
    SchemaScraperError,
)
from .generators import MarkdownGenerator


def setup_logging(verbosity: int) -> None:
    """Configure logging based on verbosity level."""
    if verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group()
@click.version_option(version=__version__)
def cli():
    """Schema Scraper - Extract database schema to markdown documentation.

    Supports: MS SQL Server, PostgreSQL, MySQL, Oracle, SQLite
    """
    pass


@cli.command()
@click.option("--db-type", "-t", type=click.Choice(SUPPORTED_BACKENDS, case_sensitive=False),
              default="mssql", help="Database type")
@click.option("-h", "--host", envvar="DB_HOST", help="Database server hostname")
@click.option("-P", "--port", type=int, envvar="DB_PORT", help="Database server port")
@click.option("-d", "--database", envvar="DB_NAME", help="Database name")
@click.option("-u", "--username", envvar="DB_USER", help="Database username")
@click.option("-p", "--password", envvar="DB_PASSWORD", help="Database password")
@click.option("--trusted", is_flag=True, help="Use Windows authentication (MSSQL only)")
@click.option("-c", "--connection-string", envvar="DB_CONNECTION_STRING",
              help="Full connection string (MSSQL only)")
@click.option("--driver", help="ODBC driver name (MSSQL only)")
@click.option("--service-name", help="Oracle service name")
@click.option("--sid", help="Oracle SID")
@click.option("-o", "--output", default="./schema_docs", type=click.Path(),
              help="Output base directory (database name will be appended)")
@click.option("--schemas", multiple=True, help="Include only specific schemas")
@click.option("--exclude-schemas", multiple=True, help="Exclude specific schemas")
@click.option("--object-types", multiple=True,
              type=click.Choice(["tables", "views", "procedures", "functions",
                               "triggers", "types", "sequences", "synonyms", "security", "all"],
                              case_sensitive=False),
              help="Object types to extract (default: all)")
@click.option("-v", "--verbose", count=True, help="Increase verbosity (-v info, -vv debug)")
@click.option("--dry-run", is_flag=True, help="Preview without writing files")
def scrape(
    db_type: str,
    host: str | None,
    port: int | None,
    database: str | None,
    username: str | None,
    password: str | None,
    trusted: bool,
    connection_string: str | None,
    driver: str | None,
    service_name: str | None,
    sid: str | None,
    output: str,
    schemas: tuple[str, ...],
    exclude_schemas: tuple[str, ...],
    object_types: tuple[str, ...],
    verbose: int,
    dry_run: bool,
) -> None:
    """Extract database schema and generate markdown documentation."""
    setup_logging(verbose)
    logger = logging.getLogger(__name__)

    try:
        # Determine database name for output folder
        if db_type.lower() == "sqlite" and database:
            # For SQLite, use the filename without extension
            db_name = Path(database).stem
        else:
            db_name = database or "unknown"

        # Sanitize database name for use in folder path
        db_name_safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in db_name)

        # Build output directory with database name
        output_dir = Path(output) / db_name_safe

        # Build configuration
        config = ScraperConfig(
            db_type=db_type.lower(),
            host=host,
            port=port,
            database=database,
            database_path=database if db_type.lower() == "sqlite" else None,
            username=username,
            password=password,
            trusted_connection=trusted,
            connection_string=connection_string,
            driver=driver,
            service_name=service_name,
            sid=sid,
            output_dir=output_dir,
            include_schemas=list(schemas),
            exclude_schemas=list(exclude_schemas) if exclude_schemas else [],
            object_types=list(object_types) if object_types else [
                "tables", "views", "procedures", "functions",
                "triggers", "types", "sequences", "synonyms"
            ],
            dry_run=dry_run,
            verbosity=verbose,
        )

        config.validate()

        # Get backend
        ConnectionClass, extractors = get_backend(db_type)

        # Connect and extract
        with ConnectionClass(config) as conn:
            db = Database(
                name=config.database or "Unknown",
                db_type=db_type,
                server=config.host,
            )

            # Get version
            if hasattr(conn, "get_version"):
                db.version = conn.get_version()

            for obj_type, ExtractorClass in extractors.items():
                if ExtractorClass is None:
                    continue
                if not config.should_extract(obj_type):
                    continue

                click.echo(f"Extracting {obj_type}...")
                extractor = ExtractorClass(conn, config)
                objects = extractor.extract()

                if obj_type == "security":
                    # Security extractor returns a dict with multiple object types
                    security_data = objects
                    for sec_type, sec_objects in security_data.items():
                        setattr(db, sec_type, sec_objects)
                        click.echo(f"  Found {len(sec_objects)} {sec_type}")
                else:
                    setattr(db, obj_type, objects)
                    click.echo(f"  Found {len(objects)} {obj_type}")

                    if obj_type == "tables":
                        # Extract triggers from tables if triggers are requested
                        if config.should_extract("triggers"):
                            all_triggers = []
                            for table in objects:
                                all_triggers.extend(table.triggers)
                            setattr(db, "triggers", all_triggers)
                            click.echo(f"  Found {len(all_triggers)} triggers")

        # Generate markdown
        click.echo("Generating markdown documentation...")
        generator = MarkdownGenerator(config)
        files = generator.generate(db)

        if dry_run:
            click.echo(f"\n[DRY RUN] Would create {len(files)} files in {config.output_dir}")
        else:
            click.echo(f"\nCreated {len(files)} files in {config.output_dir}")

    except BackendNotAvailableError as e:
        click.echo(f"Backend not available: {e}", err=True)
        sys.exit(1)
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)
    except ConnectionError as e:
        click.echo(f"Connection error: {e}", err=True)
        sys.exit(1)
    except SchemaScraperError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--db-type", "-t", type=click.Choice(SUPPORTED_BACKENDS, case_sensitive=False),
              help="Show drivers for specific database type")
def drivers(db_type: str | None) -> None:
    """List available database drivers."""
    if db_type is None or db_type.lower() == "mssql":
        click.echo("MS SQL Server ODBC Drivers:")
        try:
            import pyodbc
            all_drivers = pyodbc.drivers()
            sql_drivers = [d for d in all_drivers if "SQL" in d.upper()]
            if sql_drivers:
                for driver in sql_drivers:
                    click.echo(f"  - {driver}")
            else:
                click.echo("  None found")
        except ImportError:
            click.echo("  pyodbc not installed")

    if db_type is None or db_type.lower() == "postgresql":
        click.echo("\nPostgreSQL:")
        try:
            import psycopg
            click.echo(f"  psycopg version: {psycopg.__version__}")
        except ImportError:
            click.echo("  psycopg not installed")

    if db_type is None or db_type.lower() == "mysql":
        click.echo("\nMySQL:")
        try:
            import mysql.connector
            click.echo(f"  mysql-connector-python installed")
        except ImportError:
            click.echo("  mysql-connector-python not installed")

    if db_type is None or db_type.lower() == "oracle":
        click.echo("\nOracle:")
        try:
            import oracledb
            click.echo(f"  oracledb version: {oracledb.__version__}")
        except ImportError:
            click.echo("  oracledb not installed")

    if db_type is None or db_type.lower() == "sqlite":
        click.echo("\nSQLite:")
        import sqlite3
        click.echo(f"  sqlite3 version: {sqlite3.sqlite_version}")


@cli.command("test-connection")
@click.option("--db-type", "-t", type=click.Choice(SUPPORTED_BACKENDS, case_sensitive=False),
              default="mssql", help="Database type")
@click.option("-h", "--host", envvar="DB_HOST", help="Database server hostname")
@click.option("-P", "--port", type=int, envvar="DB_PORT", help="Database server port")
@click.option("-d", "--database", envvar="DB_NAME", help="Database name")
@click.option("-u", "--username", envvar="DB_USER", help="Database username")
@click.option("-p", "--password", envvar="DB_PASSWORD", help="Database password")
@click.option("--trusted", is_flag=True, help="Use Windows authentication (MSSQL only)")
@click.option("-c", "--connection-string", envvar="DB_CONNECTION_STRING",
              help="Full connection string (MSSQL only)")
@click.option("--driver", help="ODBC driver name (MSSQL only)")
@click.option("--service-name", help="Oracle service name")
@click.option("--sid", help="Oracle SID")
def test_connection(
    db_type: str,
    host: str | None,
    port: int | None,
    database: str | None,
    username: str | None,
    password: str | None,
    trusted: bool,
    connection_string: str | None,
    driver: str | None,
    service_name: str | None,
    sid: str | None,
) -> None:
    """Test database connection."""
    try:
        config = ScraperConfig(
            db_type=db_type.lower(),
            host=host,
            port=port,
            database=database,
            database_path=database if db_type.lower() == "sqlite" else None,
            username=username,
            password=password,
            trusted_connection=trusted,
            connection_string=connection_string,
            driver=driver,
            service_name=service_name,
            sid=sid,
        )
        config.validate()

        ConnectionClass, _ = get_backend(db_type)

        click.echo(f"Connecting to {db_type} database...")
        with ConnectionClass(config) as conn:
            version = conn.get_version() if hasattr(conn, "get_version") else "Unknown"
            click.echo("Connection successful!")
            click.echo(f"\nServer version:\n{version}")

    except BackendNotAvailableError as e:
        click.echo(f"Backend not available: {e}", err=True)
        sys.exit(1)
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)
    except ConnectionError as e:
        click.echo(f"Connection failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
