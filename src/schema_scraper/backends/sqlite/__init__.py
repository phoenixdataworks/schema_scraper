"""SQLite backend."""

from .connection import SQLiteConnection
from .extractors import (
    TableExtractor,
    TriggerExtractor,
    ViewExtractor,
)


def get_extractors() -> dict:
    """Get all extractors for SQLite."""
    return {
        "tables": TableExtractor,
        "views": ViewExtractor,
        "procedures": None,  # SQLite doesn't have stored procedures
        "functions": None,  # SQLite functions are application-defined
        "triggers": TriggerExtractor,
        "types": None,  # SQLite doesn't have user-defined types
        "sequences": None,  # SQLite uses AUTOINCREMENT
        "synonyms": None,  # SQLite doesn't have synonyms
    }


__all__ = [
    "SQLiteConnection",
    "get_extractors",
]
