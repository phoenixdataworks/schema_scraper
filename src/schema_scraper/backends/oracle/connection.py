"""Oracle database connection."""

import logging
from typing import Any, Optional

import oracledb

from ...base.connection import BaseConnection
from ...config import ScraperConfig
from ...exceptions import ConnectionError

logger = logging.getLogger(__name__)


class OracleConnection(BaseConnection):
    """Oracle connection using oracledb."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self._connection: Optional[oracledb.Connection] = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            # Build DSN
            if self.config.service_name:
                dsn = oracledb.makedsn(
                    self.config.host,
                    self.config.port or 1521,
                    service_name=self.config.service_name,
                )
            else:
                dsn = oracledb.makedsn(
                    self.config.host,
                    self.config.port or 1521,
                    sid=self.config.sid,
                )

            logger.debug(f"Connecting to Oracle: {self.config.host}:{self.config.port}")
            self._connection = oracledb.connect(
                user=self.config.username,
                password=self.config.password,
                dsn=dsn,
            )
            logger.info(f"Connected to Oracle database")
        except oracledb.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from database")

    @property
    def connection(self) -> oracledb.Connection:
        """Get the active connection."""
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        with self.connection.cursor() as cur:
            cur.execute(query, params)
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def execute_scalar(self, query: str, params: tuple = ()) -> Any:
        """Execute a query and return a single value."""
        with self.connection.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return row[0] if row else None

    def get_version(self) -> str:
        """Get Oracle version."""
        return self.execute_scalar("SELECT banner FROM v$version WHERE ROWNUM = 1") or "Unknown"
