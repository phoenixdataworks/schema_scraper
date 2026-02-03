"""PostgreSQL database connection."""

import logging
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row

from ...base.connection import BaseConnection
from ...config import ScraperConfig
from ...exceptions import ConnectionError

logger = logging.getLogger(__name__)


class PostgreSQLConnection(BaseConnection):
    """PostgreSQL connection using psycopg3."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self._connection: Optional[psycopg.Connection] = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            conn_params = {
                "host": self.config.host,
                "port": self.config.port or 5432,
                "dbname": self.config.database,
                "user": self.config.username,
            }
            if self.config.password:
                conn_params["password"] = self.config.password

            logger.debug(f"Connecting to PostgreSQL: {self.config.host}:{conn_params['port']}/{self.config.database}")
            self._connection = psycopg.connect(**conn_params)
            logger.info(f"Connected to {self.config.database}")
        except psycopg.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from database")

    @property
    def connection(self) -> psycopg.Connection:
        """Get the active connection."""
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        with self.connection.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def get_version(self) -> str:
        """Get PostgreSQL version."""
        return self.execute_scalar("SELECT version()") or "Unknown"
