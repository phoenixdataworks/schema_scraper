"""Database backend implementations."""

from typing import TYPE_CHECKING, Type

from ..exceptions import BackendNotAvailableError, ConfigurationError

if TYPE_CHECKING:
    from ..base import BaseConnection
    from ..config import ScraperConfig


def get_backend(db_type: str) -> tuple[Type["BaseConnection"], dict]:
    """
    Get the connection class and extractors for a database type.

    Returns:
        Tuple of (ConnectionClass, extractors_dict)
    """
    if db_type == "mssql":
        try:
            from .mssql import MSSQLConnection, get_extractors
            return MSSQLConnection, get_extractors()
        except ImportError as e:
            raise BackendNotAvailableError(
                f"MSSQL backend requires pyodbc. Install with: pip install schema-scraper[mssql]\n"
                f"Error: {e}"
            )

    elif db_type == "postgresql":
        try:
            from .postgresql import PostgreSQLConnection, get_extractors
            return PostgreSQLConnection, get_extractors()
        except ImportError as e:
            raise BackendNotAvailableError(
                f"PostgreSQL backend requires psycopg. Install with: pip install schema-scraper[postgresql]\n"
                f"Error: {e}"
            )

    elif db_type == "mysql":
        try:
            from .mysql import MySQLConnection, get_extractors
            return MySQLConnection, get_extractors()
        except ImportError as e:
            raise BackendNotAvailableError(
                f"MySQL backend requires mysql-connector-python. Install with: pip install schema-scraper[mysql]\n"
                f"Error: {e}"
            )

    elif db_type == "oracle":
        try:
            from .oracle import OracleConnection, get_extractors
            return OracleConnection, get_extractors()
        except ImportError as e:
            raise BackendNotAvailableError(
                f"Oracle backend requires oracledb. Install with: pip install schema-scraper[oracle]\n"
                f"Error: {e}"
            )

    elif db_type == "sqlite":
        from .sqlite import SQLiteConnection, get_extractors
        return SQLiteConnection, get_extractors()

    else:
        raise ConfigurationError(
            f"Unknown database type: {db_type}. "
            f"Supported types: mssql, postgresql, mysql, oracle, sqlite"
        )
