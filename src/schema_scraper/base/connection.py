"""Abstract base class for database connections."""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Generator

logger = logging.getLogger(__name__)


class BaseConnection(ABC):
    """Abstract base class for database connections."""

    def __init__(self, config: Any):
        self.config = config
        self._connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass

    @property
    @abstractmethod
    def connection(self) -> Any:
        """Get the active connection."""
        pass

    @contextmanager
    def cursor(self) -> Generator[Any, None, None]:
        """Get a cursor context manager."""
        cur = self.connection.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, query: str, params: tuple = ()) -> list[Any]:
        """Execute a query and return all results."""
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute_scalar(self, query: str, params: tuple = ()) -> Any:
        """Execute a query and return a single value."""
        with self.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return row[0] if row else None

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        with self.cursor() as cur:
            cur.execute(query, params)
            columns = [column[0] for column in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def __enter__(self) -> "BaseConnection":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()
