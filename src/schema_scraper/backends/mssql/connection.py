"""MS SQL Server database connection."""

import logging
import re
from typing import Any, Optional

import pyodbc

from ...base.connection import BaseConnection
from ...config import ScraperConfig
from ...exceptions import ConfigurationError, ConnectionError

logger = logging.getLogger(__name__)


class MSSQLConnection(BaseConnection):
    """MS SQL Server connection using pyodbc."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self._connection: Optional[pyodbc.Connection] = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            connection_string = self._build_connection_string()
            logger.debug(f"Connecting with: {self._mask_connection_string(connection_string)}")
            self._connection = pyodbc.connect(connection_string, timeout=30)
            logger.info(f"Connected to {self.config.database}")
        except pyodbc.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from database")

    @property
    def connection(self) -> pyodbc.Connection:
        """Get the active connection."""
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection

    def _build_connection_string(self) -> str:
        """Build a connection string from config."""
        if self.config.connection_string:
            return self.config.connection_string

        driver = self.config.driver or self._detect_driver()
        parts = [
            f"Driver={{{driver}}}",
            f"Server={self.config.host}" + (f",{self.config.port}" if self.config.port != 1433 else ""),
            f"Database={self.config.database}",
        ]

        if self.config.trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self.config.username}")
            parts.append(f"PWD={self.config.password}")

        # Trust server certificate for ODBC Driver 18+
        if "18" in driver or "19" in driver:
            parts.append("TrustServerCertificate=yes")

        return ";".join(parts)

    def _detect_driver(self) -> str:
        """Detect available ODBC driver for SQL Server."""
        drivers = pyodbc.drivers()
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server",
        ]
        for driver in preferred_drivers:
            if driver in drivers:
                return driver

        sql_drivers = [d for d in drivers if "SQL Server" in d]
        if sql_drivers:
            return sql_drivers[0]

        raise ConfigurationError(
            f"No SQL Server ODBC driver found. Available drivers: {drivers}"
        )

    def _mask_connection_string(self, conn_str: str) -> str:
        """Mask sensitive parts of connection string for logging."""
        return re.sub(r"(PWD=)[^;]+", r"\1***", conn_str)

    def get_version(self) -> str:
        """Get SQL Server version."""
        return self.execute_scalar("SELECT @@VERSION") or "Unknown"
