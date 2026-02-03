"""MySQL database connection."""

import logging
from typing import Any, Optional

import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector.cursor import MySQLCursorDict

from ...base.connection import BaseConnection
from ...config import ScraperConfig
from ...exceptions import ConnectionError

logger = logging.getLogger(__name__)


class MySQLConnection(BaseConnection):
    """MySQL connection using mysql-connector-python."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self._connection: Optional[mysql.connector.MySQLConnection] = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            conn_params = {
                "host": self.config.host,
                "port": self.config.port or 3306,
                "database": self.config.database,
                "user": self.config.username,
            }
            if self.config.password:
                conn_params["password"] = self.config.password

            logger.debug(f"Connecting to MySQL: {self.config.host}:{conn_params['port']}/{self.config.database}")
            self._connection = mysql.connector.connect(**conn_params)
            logger.info(f"Connected to {self.config.database}")
        except MySQLError as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from database")

    @property
    def connection(self) -> mysql.connector.MySQLConnection:
        """Get the active connection."""
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries with lowercase keys."""
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            # Normalize keys to lowercase for consistency
            return [{k.lower(): v for k, v in row.items()} for row in rows]
        finally:
            cursor.close()

    def execute_scalar(self, query: str, params: tuple = ()) -> Any:
        """Execute a query and return a single value."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()

    def get_version(self) -> str:
        """Get MySQL version."""
        return self.execute_scalar("SELECT VERSION()") or "Unknown"
