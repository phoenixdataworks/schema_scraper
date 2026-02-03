"""SQLite database connection."""

import logging
import sqlite3
from typing import Any, Optional

from ...base.connection import BaseConnection
from ...config import ScraperConfig
from ...exceptions import ConnectionError

logger = logging.getLogger(__name__)


class SQLiteConnection(BaseConnection):
    """SQLite connection using sqlite3."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self._connection: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            db_path = self.config.database_path or self.config.database
            logger.debug(f"Connecting to SQLite: {db_path}")
            self._connection = sqlite3.connect(db_path)
            self._connection.row_factory = sqlite3.Row
            logger.info(f"Connected to {db_path}")
        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from database")

    @property
    def connection(self) -> sqlite3.Connection:
        """Get the active connection."""
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            columns = [description[0] for description in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
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
        """Get SQLite version."""
        return self.execute_scalar("SELECT sqlite_version()") or "Unknown"
