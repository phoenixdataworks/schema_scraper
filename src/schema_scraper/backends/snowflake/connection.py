"""Snowflake database connection."""

import logging
from pathlib import Path
from typing import Any, Optional

import snowflake.connector

from ...base.connection import BaseConnection
from ...config import ScraperConfig
from ...exceptions import ConfigurationError, ConnectionError

logger = logging.getLogger(__name__)


class SnowflakeConnection(BaseConnection):
    """Snowflake connection using snowflake-connector-python."""

    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self._connection: Optional[snowflake.connector.SnowflakeConnection] = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            conn_params = self._build_connection_params()
            logger.debug(
                f"Connecting to Snowflake: {conn_params.get('account')}/"
                f"{conn_params.get('database')}"
            )
            self._connection = snowflake.connector.connect(**conn_params)
            logger.info(f"Connected to {self.config.database}")
        except snowflake.connector.errors.Error as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {e}") from e

    def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from Snowflake")

    @property
    def connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get the active connection."""
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection

    def _build_connection_params(self) -> dict[str, Any]:
        """Build connection parameters from config."""
        params: dict[str, Any] = {
            "account": self.config.snowflake_account,
            "user": self.config.username,
            "database": self.config.database,
        }

        # Key-pair authentication
        if self.config.snowflake_private_key_path:
            params["private_key"] = self._load_private_key(
                self.config.snowflake_private_key_path
            )
        elif self.config.password:
            params["password"] = self.config.password
        else:
            raise ConfigurationError(
                "Either private_key_path or password is required for Snowflake"
            )

        if self.config.snowflake_warehouse:
            params["warehouse"] = self.config.snowflake_warehouse

        if self.config.snowflake_role:
            params["role"] = self.config.snowflake_role

        return params

    def _load_private_key(self, key_path: str) -> bytes:
        """Load and deserialize a private key from a .p8 file."""
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        key_file = Path(key_path)
        if not key_file.exists():
            raise ConfigurationError(f"Private key file not found: {key_path}")

        with open(key_file, "rb") as f:
            p_key = serialization.load_pem_private_key(
                f.read(),
                password=None,
                backend=default_backend(),
            )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        with self.cursor() as cur:
            cur.execute(query, params)
            if cur.description is None:
                return []
            columns = [col[0] for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_version(self) -> str:
        """Get Snowflake version."""
        return self.execute_scalar("SELECT CURRENT_VERSION()") or "Unknown"
